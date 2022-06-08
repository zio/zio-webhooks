package zio.webhooks

import zio._
import zio.Clock
import zio.json._
import zio.stream.ZStream
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.internal._
import zio.Console

/**
 * A [[WebhookServer]] is a stateful server that subscribes to [[WebhookEvent]]s and reliably
 * delivers them, i.e. failed dispatches are retried once, followed by retries with exponential
 * backoff. Retries are performed until some duration after which webhooks will be marked
 * [[WebhookStatus.Unavailable]] since some `Instant`. Dispatches are batched if and
 * only if a batching capacity is configured ''and'' a webhook's delivery batching is
 * [[WebhookDeliveryBatching.Batched]]. When `shutdown` is called, a `shutdownSignal` is sent
 * which lets all dispatching work finish. Finally, the retry state is persisted, which allows
 * retries to resume after server restarts.
 *
 * A `live` server layer is provided in the companion object for convenience and proper resource
 * management, ensuring `shutdown` is called by the finalizer.
 */
final class WebhookServer private (
  private val clock: Clock,
  private val config: WebhookServerConfig,
  private val console: Console,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val stateRepo: WebhookStateRepo,
  private val errorHub: Hub[WebhookError],
  private val fatalPromise: Promise[Cause[Nothing], Nothing],
  private val retryController: RetryController,
  private val serializePayload: SerializePayload,
  private val startupLatch: CountDownLatch,
  private val shutdownLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhooksProxy: WebhooksProxy,
  private val webhookQueues: Ref.Synchronized[Map[WebhookId, Queue[WebhookEvent]]]
) {

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook's endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, events delivered to
   * at-least-once webhooks are enqueued for retrying, while dispatches to at-most-once webhooks are
   * marked failed.
   */
  private def deliver(dispatch: WebhookDispatch): UIO[Unit] = {
    val (payload, headers) = serializePayload(dispatch.payload, dispatch.contentType)
    val request            = WebhookHttpRequest(dispatch.url, payload, dispatch.headers ++ headers)
    for {
      response <- httpClient.post(request).either
      _        <- (dispatch.deliverySemantics, response) match {
                    case (_, Left(Left(badWebhookUrlError)))  =>
                      errorHub.publish(badWebhookUrlError)
                    case (_, Right(WebhookHttpResponse(200))) =>
                      markDispatch(dispatch, WebhookEventStatus.Delivered)
                    case (AtMostOnce, _)                      =>
                      markDispatch(dispatch, WebhookEventStatus.Failed)
                    case (AtLeastOnce, _)                     =>
                      dispatch.payload match {
                        case WebhookPayload.Single(event)   =>
                          retryController.enqueueRetry(event)
                        case WebhookPayload.Batched(events) =>
                          retryController.enqueueRetryMany(events)
                      }
                  }
    } yield ()
  }

  private def deliverEvent(batchDispatcher: Option[BatchDispatcher], event: WebhookEvent, webhook: Webhook): UIO[Any] =
    (batchDispatcher, webhook.batching) match {
      case (Some(batchDispatcher), WebhookDeliveryBatching.Batched) =>
        batchDispatcher.enqueueEvent(event)
      case _                                                        =>
        deliver(WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, WebhookPayload.Single(event)))
    }

  private def enqueueNewEvent(batchDispatcher: Option[BatchDispatcher], event: WebhookEvent): UIO[Unit] = {
    val webhookId = event.key.webhookId
    for {
      webhookQueue <- webhookQueues.modifyZIO { map =>
                        map.get(webhookId) match {
                          case Some(queue) =>
                            ZIO.succeed((queue, map))
                          case None        =>
                            for {
                              queue <- Queue.dropping[WebhookEvent](config.webhookQueueCapacity)
                              _     <- ZStream
                                         .fromQueue(queue)
                                         .foreach(handleNewEvent(batchDispatcher, _))
                                         .onError(fatalPromise.fail)
                                         .fork
                            } yield (queue, map + (webhookId -> queue))
                        }
                      }
      accepted     <- webhookQueue.offer(event)
      _            <- console
                        .printLine(
                          s"""Slow webhook detected with id "${webhookId.value}"""" +
                            " and event " + event
                        )
                        .unless(accepted)
                        .orDie
    } yield ()
  }

  private def handleNewEvent(batchDispatcher: Option[BatchDispatcher], event: WebhookEvent): UIO[Unit] = {
    val webhookId = event.key.webhookId
    for {
      isRetrying         <- retryController.isActive(webhookId)
      webhook            <- webhooksProxy.getWebhookById(webhookId)
      isShutDown         <- shutdownSignal.isDone
//      _ <- Console.printLine("shutdown signal is done").orDie
      attemptRetryEnqueue = retryController.enqueueRetry(event).race(shutdownSignal.await)
      attemptDelivery     = eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivering) *>
                              (if (isRetrying && webhook.deliveryMode.semantics == AtLeastOnce)
                                 attemptRetryEnqueue.unless(isShutDown)
                               else
                                 deliverEvent(batchDispatcher, event, webhook))
      _                  <- attemptDelivery.when(webhook.isEnabled)
    } yield ()
  }

  /**
   * Sets the new event status of all the events in a dispatch.
   */
  private def markDispatch(dispatch: WebhookDispatch, newStatus: WebhookEventStatus): UIO[Unit] =
    dispatch.payload match {
      case WebhookPayload.Single(event)      =>
        eventRepo.setEventStatus(event.key, newStatus)
      case batch @ WebhookPayload.Batched(_) =>
        eventRepo.setEventStatusMany(batch.keys, newStatus)
    }

  /**
   * Starts the webhook server by starting the following concurrently:
   *
   *   - dispatch retry monitoring
   *   - event recovery for webhooks with at-least-once delivery semantics
   *   - new webhook event subscription
   *
   * The server waits for event recovery and new event subscription to get ready, signalling that
   * the server is ready to accept events.
   */
  private def start: URIO[Scope, Unit] =
    for {
      f1 <- startRetryMonitoring
      f2 <- startEventRecovery
      f3 <- startNewEventSubscription
      f4 <- fatalPromise.await.forkScoped
      _  <- ZIO
              .raceAll(f1.await, List(f2.await, f3.await, f4.await))
              .flatMap {
                case Exit.Failure(cause) =>
                  val flatCause = cause.flatten
                  errorHub.publish(FatalError(flatCause)).when(flatCause.isDie || flatCause.isFailure)
                case _                   =>
                  ZIO.unit
              }
              .forkScoped
      _  <- startupLatch.await
    } yield ()

  /**
   * Starts the recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * at-least-once delivery semantics. Loads the state of a [[RetryController]] then enqueues events into the
   * retry queue for its webhook.
   *
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery =
    ({
      for {
        _ <- stateRepo.loadState.flatMap(
               ZIO
                 .foreachDiscard(_)(jsonState =>
                   ZIO
                     .fromEither(jsonState.fromJson[PersistentRetries])
                     .mapError(message => InvalidStateError(jsonState, message))
                     .flatMap(retryController.loadRetries)
                 )
                 .catchAll(errorHub.publish)
             )
        f <- mergeShutdown(eventRepo.recoverEvents, shutdownSignal)
               .foreach(retryController.enqueueRetry)
               .ensuring(Console.printLine("shutdownLatch.countDown 1").orDie *> shutdownLatch.countDown)
               .forkScoped
      } yield f
    } <* startupLatch.countDown)

  /**
   * Starts server subscription to new [[WebhookEvent]]s. Counts down on the `startupLatch`,
   * signalling that it's ready to accept new events.
   */
  private def startNewEventSubscription =
    eventRepo.subscribeToNewEvents.flatMap { eventDequeue =>
      for {
        // signal that the server is ready to accept new webhook events
        _               <- eventDequeue.poll *> startupLatch.countDown
        deliverFunc      = (dispatch: WebhookDispatch, _: Queue[WebhookEvent]) => deliver(dispatch)
        batchDispatcher <- ZIO.foreach(config.batchingCapacity)(
                             BatchDispatcher
                               .create(_, deliverFunc, fatalPromise, shutdownSignal, webhooksProxy)
                               .tap(_.start.fork)
                           )
        handleEvent      = for {
                             event <- (shutdownSignal.await raceEither eventDequeue.take).map(_.toOption)
                             _     <- ZIO.foreachDiscard(event)(enqueueNewEvent(batchDispatcher, _))
                           } yield ()
        isShutdown      <- shutdownSignal.isDone
//        _ <- Console.printLine("shutdown signal is done").orDie
        _               <- handleEvent
                             .repeatUntilZIO(_ => shutdownSignal.isDone)
                             .unless(isShutdown)
                             .ensuring(Console.printLine("shutdownLatch.countDown 2").orDie *> shutdownLatch.countDown)
      } yield ()
    }.forkScoped

  /**
   * Listens for new retries and starts retrying delivers to a webhook.
   */
  private def startRetryMonitoring =
    retryController.start.ensuring(Console.printLine("shutdownLatch.countDown 3").orDie *> shutdownLatch.countDown).forkScoped

  /**
   * Waits until all work in progress is finished, persists retries, then shuts down.
   */
  private def shutdown: UIO[Any] =
    for {
      _               <- shutdownSignal.succeed(())
      _               <- shutdownLatch.await
      persistentState <- clock.instant.flatMap(retryController.persistRetries)
      _               <- stateRepo.setState(persistentState.toJson)
    } yield ()

  /**
   * Exposes a way to listen for [[WebhookError]]s. This provides clients a way to handle server
   * errors that would otherwise just fail silently.
   */
  def subscribeToErrors: URIO[Scope, Dequeue[WebhookError]] =
    errorHub.subscribe
}

object WebhookServer {

  /**
   * Creates a server, pulling dependencies from the environment then initializing internal state.
   */
  private def create: URIO[Scope with Env, WebhookServer] =
    for {
      clock            <- ZIO.clock
      config           <- ZIO.service[WebhookServerConfig]
      console          <- ZIO.console
      eventRepo        <- ZIO.service[WebhookEventRepo]
      httpClient       <- ZIO.service[WebhookHttpClient]
      serializePayload <- ZIO.service[SerializePayload]
      webhookState     <- ZIO.service[WebhookStateRepo]
      webhooksProxy    <- ZIO.service[WebhooksProxy]
      errorHub         <- Hub.sliding[WebhookError](config.errorSlidingCapacity)
      fatalPromise     <- Promise.make[Cause[Nothing], Nothing]
      retryDispatchers <- Ref.Synchronized.make(Map.empty[WebhookId, RetryDispatcher])
      retryInputQueue  <- Queue.bounded[WebhookEvent](1)
      retryStates      <- Ref.Synchronized.make(Map.empty[WebhookId, RetryState])
      // startup & shutdown sync points: new event sub + event recovery + retrying
      startupLatch     <- CountDownLatch.make(3)
      shutdownLatch    <- CountDownLatch.make(3)
      shutdownSignal   <- Promise.make[Nothing, Unit]
      retries           = RetryController(
                            clock,
                            config,
                            errorHub,
                            eventRepo,
                            fatalPromise,
                            httpClient,
                            retryInputQueue,
                            retryDispatchers,
                            retryStates,
                            serializePayload,
                            shutdownLatch,
                            shutdownSignal,
                            startupLatch,
                            webhooksProxy
                          )
      webhookQueues    <- Ref.Synchronized.make(Map.empty[WebhookId, Queue[WebhookEvent]])
    } yield new WebhookServer(
      clock,
      config,
      console,
      eventRepo,
      httpClient,
      webhookState,
      errorHub,
      fatalPromise,
      retries,
      serializePayload,
      startupLatch,
      shutdownLatch,
      shutdownSignal,
      webhooksProxy,
      webhookQueues
    )

  type Env = SerializePayload
    with WebhooksProxy
    with WebhookStateRepo
    with WebhookEventRepo
    with WebhookHttpClient
    with WebhookServerConfig

  def getErrors: URIO[Scope with WebhookServer, Dequeue[WebhookError]] =
    ZIO.service[WebhookServer].flatMap(_.subscribeToErrors)

  /**
   * Creates and starts a managed server, ensuring shutdown on release.
   */
  val live: URLayer[WebhookServer.Env, WebhookServer] =
    ZLayer.scoped(WebhookServer.start)

  def start: URIO[Scope with Env, WebhookServer] =
    ZIO.acquireRelease {
      for {
        server <- create
        _      <- server.start
      } yield server
    }(server => Console.printLine("shutdown start").orDie *> server.shutdown *>  Console.printLine("shutdown end").orDie)

  def subscribeToErrors: URIO[Scope with WebhookServer, Dequeue[WebhookError]] =
    ZIO.service[WebhookServer].flatMap(_.subscribeToErrors)
}
