package zio.webhooks

import zio._
import zio.clock.Clock
import zio.json._
import zio.prelude.NonEmptySet
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.internal._

import java.io.IOException

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
final case class WebhookServer private (
  private val clock: Clock.Service,
  private val config: WebhookServerConfig,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val stateRepo: WebhookStateRepo,
  private val errorHub: Hub[WebhookError],
  private val retryController: RetryController,
  private val startupLatch: CountDownLatch,
  private val shutdownLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhooksProxy: WebhooksProxy
) {

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook's endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, events delivered to
   * at-least-once webhooks are enqueued for retrying, while dispatches to at-most-once webhooks are
   * marked failed.
   */
  private def deliver(dispatch: WebhookDispatch): UIO[Unit] =
    for {
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).either
      _        <- (dispatch.deliverySemantics, response) match {
                    case (_, Left(Left(badWebhookUrlError)))  =>
                      errorHub.publish(badWebhookUrlError)
                    case (_, Right(WebhookHttpResponse(200))) =>
                      markDispatch(dispatch, WebhookEventStatus.Delivered)
                    case (AtMostOnce, _)                      =>
                      markDispatch(dispatch, WebhookEventStatus.Failed)
                    case (AtLeastOnce, _)                     =>
                      retryController.enqueueRetryMany(dispatch.events)
                  }
    } yield ()

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
            NonEmptySet.single(event)
          )
        ).fork
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
    if (dispatch.size == 1)
      eventRepo.setEventStatus(dispatch.head.key, newStatus)
    else
      eventRepo.setEventStatusMany(dispatch.keys, newStatus)

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
  def start: UIO[Any] =
    for {
      f1 <- startRetryMonitoring
      f2 <- startEventRecovery
      f3 <- startNewEventSubscription
      _  <- ZIO.raceAll(f1.await, List(f2.await, f3.await)).fork
      _  <- startupLatch.await
    } yield ()

  /**
   * Starts the recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * at-least-once delivery semantics. Loads the state of a [[RetryController]] then enqueues events into the
   * retry queue for its webhook.
   *
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery: UIO[Fiber.Runtime[Nothing, Unit]] = {
    for {
      // FIXME: getState then clearState should be one atomic operation
      _ <- stateRepo.getState.flatMap(
             ZIO
               .foreach_(_)(jsonState =>
                 ZIO
                   .fromEither(jsonState.fromJson[PersistentRetries])
                   .mapError(message => InvalidStateError(jsonState, message))
                   .flatMap(retryController.loadRetries)
               )
               .catchAll(errorHub.publish)
           )
      _ <- stateRepo.clearState
      f <- mergeShutdown(eventRepo.recoverEvents, shutdownSignal)
             .foreach(retryController.enqueueRetry)
             .ensuring(shutdownLatch.countDown)
             .fork
    } yield f
  } <* startupLatch.countDown

  /**
   * Starts server subscription to new [[WebhookEvent]]s. Counts down on the `startupLatch`,
   * signalling that it's ready to accept new events.
   */
  private def startNewEventSubscription: UIO[Fiber.Runtime[Nothing, Unit]] =
    eventRepo.subscribeToNewEvents.use { eventDequeue =>
      for {
        // signal that the server is ready to accept new webhook events
        _               <- eventDequeue.poll *> startupLatch.countDown
        deliverFunc      = (dispatch: WebhookDispatch, _: Queue[WebhookEvent]) => deliver(dispatch)
        batchDispatcher <- ZIO.foreach(config.batchingCapacity)(
                             BatchDispatcher
                               .create(_, deliverFunc, shutdownSignal, webhooksProxy)
                               .tap(_.start.fork)
                           )
        handleEvent      = for {
                             event <- (shutdownSignal.await raceEither eventDequeue.take).map(_.toOption)
                             _     <- ZIO.foreach_(event)(handleNewEvent(batchDispatcher, _))
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
  private def startRetryMonitoring: UIO[Fiber.Runtime[Nothing, Any]] =
    retryController.start.ensuring(shutdownLatch.countDown).fork

  /**
   * Waits until all work in progress is finished, persists retries, then shuts down.
   */
  def shutdown: UIO[Any] =
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
  private def create: URIO[Env, WebhookServer] =
    for {
      clock            <- ZIO.service[Clock.Service]
      config           <- ZIO.service[WebhookServerConfig]
      eventRepo        <- ZIO.service[WebhookEventRepo]
      httpClient       <- ZIO.service[WebhookHttpClient]
      webhookState     <- ZIO.service[WebhookStateRepo]
      webhooks         <- ZIO.service[WebhooksProxy]
      errorHub         <- Hub.sliding[WebhookError](config.errorSlidingCapacity)
      retryDispatchers <- RefM.make(Map.empty[WebhookId, RetryDispatcher])
      retryInputQueue  <- Queue.bounded[WebhookEvent](1)
      retryStates      <- RefM.make(Map.empty[WebhookId, RetryState])
      // startup & shutdown sync points: new event sub + event recovery + retrying
      startupLatch     <- CountDownLatch.make(3)
      shutdownLatch    <- CountDownLatch.make(3)
      shutdownSignal   <- Promise.make[Nothing, Unit]
      retries           = RetryController(
                            clock,
                            config,
                            errorHub,
                            eventRepo,
                            httpClient,
                            retryInputQueue,
                            retryDispatchers,
                            retryStates,
                            shutdownLatch,
                            shutdownSignal,
                            startupLatch,
                            webhooks
                          )
    } yield WebhookServer(
      clock,
      config,
      eventRepo,
      httpClient,
      webhookState,
      errorHub,
      retries,
      startupLatch,
      shutdownLatch,
      shutdownSignal,
      webhooks
    )

  type Env = Has[WebhooksProxy]
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
  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      server <- WebhookServer.start.toManaged_
      _      <- ZManaged.finalizer(server.shutdown)
    } yield server
  }.toLayer

  /**
   * Accessor method for manually shutting down a managed server.
   */
  def shutdown: ZIO[Has[WebhookServer], IOException, Any] =
    ZIO.service[WebhookServer].flatMap(_.shutdown) // serviceWith doesn't compile

  def start: URIO[Env, WebhookServer] =
    create.tap(_.start)

  def subscribeToErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.subscribeToErrors)
}
