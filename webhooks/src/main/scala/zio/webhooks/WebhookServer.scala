package zio.webhooks

import zio._
import zio.clock.Clock
import zio.json._
import zio.stream.UStream
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.internal._

/**
 * A [[WebhookServer]] is a stateful server that subscribes to [[WebhookEvent]]s and reliably
 * delivers them, i.e. failed dispatches are retried once, followed by retries with exponential
 * backoff. Retries are performed until some duration after which webhooks will be marked
 * [[WebhookStatus.Unavailable]] since some [[java.time.Instant]]. Dispatches are batched if and
 * only if a batching capacity is configured ''and'' a webhook's delivery batching is
 * [[WebhookDeliveryBatching.Batched]]. When [[shutdown]] is called, a [[shutdownSignal]] is sent
 * which lets all dispatching work finish. Finally, the retry state is persisted, which allows
 * retries to resume after server restarts.
 *
 * A `live` server layer is provided in the companion object for convenience and proper resource
 * management, ensuring [[shutdown]] is called by the finalizer.
 */
final class WebhookServer private (
  private val clock: Clock.Service,
  private val config: WebhookServerConfig,
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
  private val webhookQueues: RefM[Map[WebhookId, Queue[WebhookEvent]]]
) {

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook's endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, events delivered to
   * at-least-once webhooks are enqueued for retrying, while dispatches to at-most-once webhooks are
   * marked failed.
   */
  private def deliver(dispatch: WebhookDispatch): UIO[Unit] = {
    val request =
      WebhookHttpRequest(dispatch.url, serializePayload(dispatch.payload, dispatch.contentType), dispatch.headers)
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
        deliver(
          WebhookDispatch(
            webhook.id,
            webhook.url,
            webhook.deliveryMode.semantics,
            WebhookPayload.Single(event)
          )
        ).fork
    }

  private def enqueueNewEvent(batchDispatcher: Option[BatchDispatcher], event: WebhookEvent): UIO[Unit] = {
    val webhookId = event.key.webhookId
    for {
      webhookQueue <- webhookQueues.modify { map =>
                        map.get(webhookId) match {
                          case Some(queue) =>
                            UIO((queue, map))
                          case None        =>
                            for {
                              queue <- Queue.dropping[WebhookEvent](config.webhookQueueCapacity)
                              _     <- UStream
                                         .fromQueue(queue)
                                         .foreach(handleNewEvent(batchDispatcher, _))
                                         .onError(fatalPromise.fail)
                                         .fork
                            } yield (queue, map + (webhookId -> queue))
                        }
                      }
      _            <- webhookQueue.offer(event)
    } yield ()
  }

  private def handleNewEvent(batchDispatcher: Option[BatchDispatcher], event: WebhookEvent): UIO[Unit] = {
    val webhookId = event.key.webhookId
    for {
      isRetrying         <- retryController.isActive(webhookId)
      webhook            <- webhooksProxy.getWebhookById(webhookId)
      isShutDown         <- shutdownSignal.isDone
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
  private def start: UManaged[Unit] =
    for {
      f1 <- startRetryMonitoring
      f2 <- startEventRecovery
      f3 <- startNewEventSubscription
      f4 <- fatalPromise.await.forkManaged
      _  <- ZIO
              .raceAll(f1.await, List(f2.await, f3.await, f4.await))
              .flatMap {
                case Exit.Failure(cause) =>
                  val flatCause = cause.flatten
                  errorHub.publish(FatalError(flatCause)).when(flatCause.died || flatCause.failed)
                case _                   =>
                  ZIO.unit
              }
              .forkManaged
      _  <- startupLatch.await.toManaged_
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
                 .foreach_(_)(jsonState =>
                   ZIO
                     .fromEither(jsonState.fromJson[PersistentRetries])
                     .mapError(message => InvalidStateError(jsonState, message))
                     .flatMap(retryController.loadRetries)
                 )
                 .catchAll(errorHub.publish)
             )
        f <- mergeShutdown(eventRepo.recoverEvents, shutdownSignal)
               .foreach(retryController.enqueueRetry)
               .ensuring(shutdownLatch.countDown)
               .fork
      } yield f
    } <* startupLatch.countDown).toManaged_

  /**
   * Starts server subscription to new [[WebhookEvent]]s. Counts down on the `startupLatch`,
   * signalling that it's ready to accept new events.
   */
  private def startNewEventSubscription =
    eventRepo.subscribeToNewEvents.mapM { eventDequeue =>
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
                             _     <- ZIO.foreach_(event)(enqueueNewEvent(batchDispatcher, _))
                           } yield ()
        isShutdown      <- shutdownSignal.isDone
        _               <- handleEvent
                             .repeatUntilM(_ => shutdownSignal.isDone)
                             .unless(isShutdown)
                             .ensuring(shutdownLatch.countDown)
      } yield ()
    }.fork

  /**
   * Listens for new retries and starts retrying delivers to a webhook.
   */
  private def startRetryMonitoring =
    retryController.start.ensuring(shutdownLatch.countDown).forkManaged

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
  def subscribeToErrors: UManaged[Dequeue[WebhookError]] =
    errorHub.subscribe
}

object WebhookServer {

  /**
   * Creates a server, pulling dependencies from the environment then initializing internal state.
   */
  private def create: URManaged[Env, WebhookServer] =
    for {
      clock            <- ZManaged.service[Clock.Service]
      config           <- ZManaged.service[WebhookServerConfig]
      eventRepo        <- ZManaged.service[WebhookEventRepo]
      httpClient       <- ZManaged.service[WebhookHttpClient]
      serializePayload <- ZManaged.service[SerializePayload]
      webhookState     <- ZManaged.service[WebhookStateRepo]
      webhooksProxy    <- ZManaged.service[WebhooksProxy]
      errorHub         <- Hub.sliding[WebhookError](config.errorSlidingCapacity).toManaged_
      fatalPromise     <- Promise.makeManaged[Cause[Nothing], Nothing]
      retryDispatchers <- RefM.makeManaged(Map.empty[WebhookId, RetryDispatcher])
      retryInputQueue  <- Queue.bounded[WebhookEvent](1).toManaged_
      retryStates      <- RefM.makeManaged(Map.empty[WebhookId, RetryState])
      // startup & shutdown sync points: new event sub + event recovery + retrying
      startupLatch     <- CountDownLatch.make(3).toManaged_
      shutdownLatch    <- CountDownLatch.make(3).toManaged_
      shutdownSignal   <- Promise.makeManaged[Nothing, Unit]
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
      webhookQueues    <- RefM.makeManaged(Map.empty[WebhookId, Queue[WebhookEvent]])
    } yield new WebhookServer(
      clock,
      config,
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

  type Env = Has[SerializePayload]
    with Has[WebhooksProxy]
    with Has[WebhookStateRepo]
    with Has[WebhookEventRepo]
    with Has[WebhookHttpClient]
    with Has[WebhookServerConfig]
    with Clock

  def getErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.subscribeToErrors)

  /**
   * Creates and starts a managed server, ensuring shutdown on release.
   */
  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] =
    WebhookServer.start.toLayer

  def start: URManaged[Env, WebhookServer] =
    for {
      server <- create
      _      <- server.start
      _      <- ZManaged.finalizer(server.shutdown)
    } yield server

  def subscribeToErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.subscribeToErrors)
}
