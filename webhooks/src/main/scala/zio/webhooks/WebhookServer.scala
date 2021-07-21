package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.json._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer._
import zio.webhooks.internal._

import java.io.IOException
import java.time.{ Instant, Duration => JDuration }

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
 * A [[live]] server layer is provided in the companion object for convenience and proper resource
 * management, ensuring [[shutdown]] is called by the finalizer.
 */
final class WebhookServer private (
  private val clock: Clock.Service,
  private val config: WebhookServerConfig,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val stateRepo: WebhookStateRepo,
  private val errorHub: Hub[WebhookError],
  private val newRetries: Queue[RetryState],
  private val permits: Semaphore,
  private val retries: RefM[Retries],
  private val startupLatch: CountDownLatch,
  private val shutdownLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhooksProxy: WebhooksProxy
) {

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook's endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, events delivered to
   * at-least-once webhooks with are enqueued for retrying, while dispatches to at-most-once
   * webhooks are marked failed.
   */
  private def deliver(dispatch: WebhookDispatch): UIO[Unit] = {
    for {
      _        <- markDispatch(dispatch, WebhookEventStatus.Delivering)
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).either
      _        <- (dispatch.deliverySemantics, response) match {
                    case (_, Left(Left(badWebhookUrlError)))  =>
                      errorHub.publish(badWebhookUrlError)
                    case (_, Right(WebhookHttpResponse(200))) =>
                      markDispatch(dispatch, WebhookEventStatus.Delivered)
                    case (AtMostOnce, _)                      =>
                      markDispatch(dispatch, WebhookEventStatus.Failed)
                    case (AtLeastOnce, _)                     =>
                      val webhookId = dispatch.webhookId
                      retries.update { retries =>
                        for {
                          retryState <- retries.map
                                          .get(webhookId)
                                          .fold(RetryState.make(clock, config.retry))(UIO(_))
                                          .flatMap(_.activateWithTimeout(markWebhookUnavailable(webhookId)))
                          _          <- retryState.enqueueAll(dispatch.events)
                          _          <- newRetries.offer(retryState)
                        } yield retries.updateRetryState(webhookId, retryState)
                      }
                  }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  private def deliverNewEvent(newEvent: WebhookEvent): UIO[Unit] = {
    for {
      webhook <- webhooksProxy.getWebhookById(newEvent.key.webhookId)
      dispatch = WebhookDispatch(
                   webhook.id,
                   webhook.url,
                   webhook.deliveryMode.semantics,
                   NonEmptySet(newEvent)
                 )
      _       <- deliver(dispatch).when(webhook.isEnabled)
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  /**
   * Sets the new event status of all the events in a dispatch.
   */
  private def markDispatch(dispatch: WebhookDispatch, newStatus: WebhookEventStatus): IO[WebhookError, Unit] =
    if (dispatch.size == 1)
      eventRepo.setEventStatus(dispatch.head.key, newStatus)
    else
      eventRepo.setEventStatusMany(dispatch.keys, newStatus)

  /**
   * Marks a webhook unavailable, marking all its events failed.
   */
  private def markWebhookUnavailable(webhookId: WebhookId): IO[WebhookError, Unit] =
    for {
      _                 <- eventRepo.setAllAsFailedByWebhookId(webhookId)
      unavailableStatus <- clock.instant.map(WebhookStatus.Unavailable)
      _                 <- webhooksProxy.setWebhookStatus(webhookId, unavailableStatus)
    } yield ()

  /**
   * Loads the persisted retry states and resumes them.
   */
  private def loadRetries(loadedState: PersistentRetries): IO[WebhookError, Unit] =
    for {
      retryMap <- ZIO.foreach(loadedState.retryStates) {
                    case (id, persistedState) =>
                      resumeRetrying(WebhookId(id), persistedState).map((WebhookId(id), _))
                  }
      _        <- retries.set(Retries(retryMap))
      _        <- ZIO.foreach_(retryMap) { case (_, retryState) => newRetries.offer(retryState) }
    } yield ()

  /**
   * Recovers an event by adding it to a retry queue, or starting retry dispatch for a webhook if
   * retries for it haven't started yet.
   */
  private def recoverEvent(event: WebhookEvent): UIO[Unit] =
    for {
      retryQueue <- retries.modify { state =>
                      state.map.get(event.key.webhookId) match {
                        // we're continuing retries for this webhook
                        case Some(retryState) =>
                          UIO((Some(retryState.retryQueue), state))
                        // no retry state was loaded for this webhook, make a new one
                        case None             =>
                          RetryState.make(clock, config.retry).map { retryState =>
                            (Some(retryState.retryQueue), state.updateRetryState(event.key.webhookId, retryState))
                          }
                      }
                    }
      _          <- ZIO.foreach_(retryQueue)(_.offer(event))
    } yield ()

  /**
   * Resumes retries for a webhook given a persisted retry state loaded on startup.
   */
  private def resumeRetrying(
    webhookId: WebhookId,
    persistedState: PersistentRetries.RetryingState
  ): IO[WebhookError, RetryState] =
    for {
      loadedState  <- ZIO.mapN(
                        Queue.bounded[WebhookEvent](config.retry.capacity),
                        Queue.bounded[Promise[Nothing, Unit]](config.retry.capacity)
                      ) { (retryQueue, backoffResets) =>
                        RetryState(
                          clock,
                          retryQueue,
                          backoffResets,
                          config.retry.exponentialBase,
                          config.retry.exponentialPower,
                          config.retry.maxBackoff,
                          persistedState.timeLeft,
                          persistedState.activeSinceTime,
                          persistedState.failureCount,
                          inFlight = Map.empty,
                          isActive = false,
                          lastRetryTime = persistedState.lastRetryTime,
                          nextBackoff = persistedState.backoff,
                          timerKillSwitch = None
                        )
                      }
      resumedRetry <- loadedState.activateWithTimeout(markWebhookUnavailable(webhookId))
    } yield resumedRetry

  /**
   * Attempts to retry a dispatch. Each attempt updates the retry state based on
   * the outcome of the attempt.
   *
   * Each failed attempt causes the retry backoff to increase exponentially, so as not to flood the
   * endpoint with retry attempts.
   *
   * On the other hand, each successful attempt resets backoff—allowing for greater throughput
   * for retries when the endpoint begins to return `200` status codes.
   */
  private def retryEvents(
    retryState: Ref[RetryState],
    dispatch: WebhookDispatch,
    batchQueue: Option[Queue[WebhookEvent]] = None
  ): UIO[Unit] = {
    for {
      _        <- retryState.update(_.addInFlight(dispatch.events))
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).either
      _        <- response match {
                    case Left(Left(badWebhookUrlError))  =>
                      errorHub.publish(badWebhookUrlError)
                    case Right(WebhookHttpResponse(200)) =>
                      for {
                        _                <- retryState.update(_.removeInFlight(dispatch.events))
                        _                <- markDispatch(dispatch, WebhookEventStatus.Delivered)
                        now              <- clock.instant
                        newState         <- retries.modify { retries =>
                                              for {
                                                newState <- retryState.updateAndGet(_.resetBackoff(now))
                                                _        <- retryState.get.flatMap {
                                                              _.backoffResets.takeAll
                                                                .flatMap(ZIO.foreach_(_)(_.succeed(())))
                                                            }
                                              } yield (
                                                newState,
                                                retries.updateRetryState(dispatch.webhookId, newState)
                                              )
                                            }
                        queueEmpty       <- newState.retryQueue.size.map(_ <= 0)
                        batchExistsEmpty <- ZIO.foreach(batchQueue)(_.size.map(_ <= 0)).map(_.getOrElse(true))
                        inFlightEmpty    <- retryState.get.map(_.inFlight.isEmpty)
                        allEmpty          = queueEmpty && inFlightEmpty && batchExistsEmpty
                        setInactive       = retryState.get.flatMap(_.deactivate)
                        _                <- setInactive.when(allEmpty)
                      } yield ()
                    // retry responded with a non-200 status, or an IOException occurred
                    case _                               =>
                      for {
                        timestamp <- clock.instant
                        nextState <- retryState.updateAndGet(_.increaseBackoff(timestamp))
                        _         <- retries.update(state => UIO(state.updateRetryState(dispatch.webhookId, nextState)))
                        requeue    = nextState.requeue(dispatch.events) *>
                                       retryState.update(_.removeInFlight(dispatch.events))
                        // prevent batches from getting into deadlocks by forking the requeue
                        _         <- if (batchQueue.isDefined) requeue.fork else requeue
                      } yield ()
                  }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  /**
   * Starts the webhook server by starting the following concurrently:
   *
   *   - new webhook event subscription
   *   - event recovery for webhooks with at-least-once delivery semantics
   *   - dispatch retry monitoring
   *   - webhook polling or update subscription
   *
   * The server waits for event recovery and new event subscription to get ready, signalling that
   * the server is ready to accept events.
   */
  def start: UIO[Any] =
    for {
      _ <- startEventRecovery
      _ <- startRetryMonitoring
      _ <- startNewEventSubscription
      _ <- startupLatch.await
    } yield ()

  /**
   * Starts the recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * at-least-once delivery semantics. Loads [[WebhookServer.Retries]] then enqueues events into the
   * retry queue for its webhook.
   *
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery: UIO[Any] = {
    for {
      _ <- stateRepo.getState.flatMap(
             ZIO
               .foreach_(_)(jsonState =>
                 ZIO
                   .fromEither(jsonState.fromJson[PersistentRetries])
                   .mapError(message => InvalidStateError(jsonState, message))
                   .flatMap(loadRetries)
               )
               .catchAll(errorHub.publish)
           )
      _ <- mergeShutdown(eventRepo.recoverEvents, shutdownSignal).foreach { event =>
             (for {
               webhook <- webhooksProxy.getWebhookById(event.key.webhookId)
               _       <- recoverEvent(event).when(webhook.isEnabled)
             } yield ()).catchAll(errorHub.publish)
           }
    } yield ()
  }.fork *> startupLatch.countDown

  /**
   * Starts server subscription to new [[WebhookEvent]]s. Counts down on the `startupLatch`,
   * signalling that it's ready to accept new events.
   */
  private def startNewEventSubscription: UIO[Any] =
    eventRepo.subscribeToNewEvents.use { eventDequeue =>
      for {
        // signal that the server is ready to accept new webhook events
        _               <- eventDequeue.poll *> startupLatch.countDown
        isShutdown      <- shutdownSignal.isDone
        deliverFunc      = (dispatch: WebhookDispatch, _: Queue[WebhookEvent]) => deliver(dispatch)
        batchDispatcher <- ZIO.foreach(config.batchingCapacity)(
                             BatchDispatcher
                               .create(_, deliverFunc, errorHub, shutdownSignal, webhooksProxy)
                               .tap(_.start.fork)
                           )
        handleEvent      = (shutdownSignal.await raceEither eventDequeue.take).flatMap {
                             case Left(_)      =>
                               ZIO.unit
                             case Right(event) =>
                               handleNewEvent(batchDispatcher, event)
                           }
        _               <- handleEvent
                             .catchAll(errorHub.publish(_))
                             .repeatUntilM(_ => shutdownSignal.isDone)
                             .unless(isShutdown)
        _               <- shutdownLatch.countDown
      } yield ()
    }.fork

  private def handleNewEvent(
    batchDispatcher: Option[BatchDispatcher],
    event: WebhookEvent
  ): IO[MissingWebhookError, Unit] =
    for {
      webhook <- webhooksProxy.getWebhookById(event.key.webhookId)
      _       <- ((batchDispatcher, webhook.batching) match {
                     case (Some(batchDispatcher), WebhookDeliveryBatching.Batched) =>
                       batchDispatcher.enqueueEvent(event)
                     case _                                                        =>
                       permits.withPermit(deliverNewEvent(event)).fork
                   }).when(webhook.isEnabled)
    } yield ()

  /**
   * Listens for new retries and starts retry dispatching for a webhook.
   */
  private def startRetryMonitoring: UIO[Any] = {
    mergeShutdown(UStream.fromQueue(newRetries), shutdownSignal).foreach { retryState =>
      val startRetries =
        for {
          retryState      <- Ref.make(retryState)
          deliverFunc      = (dispatch: WebhookDispatch, batchQueue: Queue[WebhookEvent]) =>
                               retryEvents(retryState, dispatch, Some(batchQueue))
          batchDispatcher <- ZIO.foreach(config.batchingCapacity)(
                               BatchDispatcher
                                 .create(_, deliverFunc, errorHub, shutdownSignal, webhooksProxy)
                                 .tap(_.start.fork)
                             )
          retryQueue      <- retryState.get.map(_.retryQueue)
          handleEvent      = (shutdownSignal.await raceEither retryQueue.take).flatMap {
                               case Left(_)      =>
                                 ZIO.unit
                               case Right(event) =>
                                 for {
                                   webhook <- webhooksProxy.getWebhookById(event.key.webhookId)
                                   _       <- (batchDispatcher, webhook.batching) match {
                                                case (Some(batchDispatcher), WebhookDeliveryBatching.Batched) =>
                                                  batchDispatcher.enqueueEvent(event)
                                                case _                                                        =>
                                                  val dispatch = WebhookDispatch(
                                                    webhook.id,
                                                    webhook.url,
                                                    webhook.deliveryMode.semantics,
                                                    NonEmptySet.single(event)
                                                  )
                                                  permits
                                                    .withPermit(retryEvents(retryState, dispatch))
                                                    .fork
                                                    .when(webhook.isEnabled)
                                              }
                                 } yield ()
                             }
          isShutdown      <- shutdownSignal.isDone
          _               <- handleEvent
                               .catchAll(errorHub.publish(_))
                               .repeatUntilM(_ => shutdownSignal.isDone)
                               .unless(isShutdown)
        } yield ()
      startRetries.fork
    } *> shutdownLatch.countDown
  }.fork

  /**
   * Waits until all work in progress is finished, persists retries, then shuts down.
   */
  def shutdown: UIO[Any] =
    for {
      _               <- shutdownSignal.succeed(())
      _               <- shutdownLatch.await
      persistentState <- retries.get.flatMap(state => clock.instant.map(state.toPersistentServerState))
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
  def create: URIO[Env, WebhookServer] =
    for {
      clock            <- ZIO.service[Clock.Service]
      config           <- ZIO.service[WebhookServerConfig]
      eventRepo        <- ZIO.service[WebhookEventRepo]
      httpClient       <- ZIO.service[WebhookHttpClient]
      webhookState     <- ZIO.service[WebhookStateRepo]
      webhooks         <- ZIO.service[WebhooksProxy]
      errorHub         <- Hub.sliding[WebhookError](config.errorSlidingCapacity)
      newRetries       <- Queue.bounded[RetryState](config.retry.capacity)
      singleDispatches <- Semaphore.make(config.maxSingleDispatchConcurrency.toLong)
      retries          <- RefM.make(Retries(Map.empty))
      // startup sync points: new event sub + event recovery
      startupLatch     <- CountDownLatch.make(2)
      // shutdown sync points: new event sub + event recovery + retrying
      shutdownLatch    <- CountDownLatch.make(2)
      shutdownSignal   <- Promise.make[Nothing, Unit]
    } yield new WebhookServer(
      clock,
      config,
      eventRepo,
      httpClient,
      webhookState,
      errorHub,
      newRetries,
      singleDispatches,
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
   * [[Retries]] is the server's internal representation of each webhook's [[RetryState]].
   */
  private[webhooks] final case class Retries(map: Map[WebhookId, RetryState]) {

    /**
     * Suspends all retrying states to prepare to save them during server shutdown.
     */
    private def suspendRetries(timestamp: Instant): Retries            =
      copy(map = map.map { case (id, retryState) => (id, retryState.suspend(timestamp)) })

    /**
     * Maps retries to [[PersistentRetries]].
     */
    def toPersistentServerState(timestamp: Instant): PersistentRetries =
      PersistentRetries(suspendRetries(timestamp).map.collect {
        case (webhookId, retryState) =>
          val persistentRetry = PersistentRetries.RetryingState(
            activeSinceTime = retryState.activeSinceTime,
            backoff = retryState.nextBackoff,
            failureCount = retryState.failureCount,
            lastRetryTime = retryState.lastRetryTime,
            timeLeft = retryState.timeoutDuration
          )
          (webhookId.value, persistentRetry)
      })

    def updateRetryState(id: WebhookId, updated: RetryState): Retries =
      copy(map = map.updated(id, updated))
  }

  /**
   * Creates and starts a managed server, ensuring shutdown on release.
   */
  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      server <- WebhookServer.create.toManaged_
      _      <- server.start.toManaged_
      _      <- ZManaged.finalizer(server.shutdown)
    } yield server
  }.toLayer

  /**
   * A [[RetryState]] represents retrying logic for a single webhook.
   */
  private[webhooks] final case class RetryState private (
    clock: Clock.Service,
    retryQueue: Queue[WebhookEvent],
    backoffResets: Queue[Promise[Nothing, Unit]],
    exponentialBaseDuration: Duration,
    exponentialPower: Double,
    maxBackoffDuration: Duration,
    timeoutDuration: Duration,
    activeSinceTime: Instant,
    failureCount: Int,
    inFlight: Map[WebhookEventKey, WebhookEvent],
    isActive: Boolean,
    lastRetryTime: Instant,
    nextBackoff: Duration,
    timerKillSwitch: Option[Promise[Nothing, Unit]]
  ) {

    /**
     * Activates a timer that calls `onTimeout` should the retry state remain active past the
     * timeout duration. The timer is killed when retrying is deactivated.
     */
    def activateWithTimeout[E](onTimeout: IO[E, Unit]): IO[E, RetryState] =
      if (isActive)
        UIO(this)
      else
        for {
          timerKillSwitch <- Promise.make[Nothing, Unit]
          runTimer         = timerKillSwitch.await
                               .timeoutTo(false)(_ => true)(timeoutDuration)
                               .flatMap(onTimeout.unless(_))
          _               <- runTimer.fork.provideLayer(ZLayer.succeed(clock))
        } yield copy(isActive = true, timerKillSwitch = Some(timerKillSwitch))

    /**
     * Adds events to a map of events that are in the middle of being retried.
     */
    def addInFlight(events: Iterable[WebhookEvent]): RetryState =
      copy(inFlight = inFlight ++ events.map(ev => ev.key -> ev))

    /**
     * Kills the current timer, marking this retry inactive.
     */
    def deactivate: UIO[RetryState]                             =
      ZIO.foreach_(timerKillSwitch)(_.succeed(())).as(copy(isActive = false, timerKillSwitch = None))

    /**
     * A convenience method for adding events to the retry queue.
     */
    def enqueueAll(events: Iterable[WebhookEvent]): UIO[Boolean] =
      retryQueue.offerAll(events)

    /**
     * Progresses retrying to the next exponential backoff.
     */
    def increaseBackoff(timestamp: Instant): RetryState = {
      val nextExponential = exponentialBaseDuration * math.pow(2, failureCount.toDouble)
      val nextBackoff     = if (nextExponential >= maxBackoffDuration) maxBackoffDuration else nextExponential
      val nextAttempt     = if (nextExponential >= maxBackoffDuration) failureCount else failureCount + 1
      copy(failureCount = nextAttempt, lastRetryTime = timestamp, nextBackoff = nextBackoff)
    }

    /**
     * Removes events from this retry state's map of events in-flight.
     */
    def removeInFlight(events: Iterable[WebhookEvent]): RetryState =
      copy(inFlight = inFlight.removeAll(events.map(_.key)))

    def requeue(events: NonEmptySet[WebhookEvent]): UIO[Unit] =
      for {
        backoffReset <- Promise.make[Nothing, Unit]
        _            <- backoffResets.offer(backoffReset)
        _            <- clock.sleep(nextBackoff) race backoffReset.await
        _            <- retryQueue.offerAll(events)
      } yield ()

    /**
     * Reverts retry backoff to the initial state.
     */
    def resetBackoff(timestamp: Instant): RetryState =
      copy(failureCount = 0, lastRetryTime = timestamp, nextBackoff = exponentialBaseDuration)

    /**
     * Suspends this retry by replacing the backoff with the time left until its backoff completes.
     */
    def suspend(now: Instant): RetryState =
      copy(
        timeoutDuration = timeoutDuration.minus(Duration.fromInterval(activeSinceTime, now)),
        nextBackoff = nextBackoff.minus(JDuration.between(now, lastRetryTime))
      )
  }

  private[webhooks] object RetryState {
    def make(clock: Clock.Service, retryConfig: WebhookServerConfig.Retry): UIO[RetryState] =
      ZIO.mapN(
        Queue.bounded[WebhookEvent](retryConfig.capacity),
        Queue.bounded[Promise[Nothing, Unit]](retryConfig.capacity),
        clock.instant
      )((retryQueue, backoffResetsQueue, timestamp) =>
        RetryState(
          clock,
          retryQueue,
          backoffResetsQueue,
          retryConfig.exponentialBase,
          retryConfig.exponentialPower,
          maxBackoffDuration = retryConfig.maxBackoff,
          timeoutDuration = retryConfig.timeout,
          activeSinceTime = timestamp,
          failureCount = 0,
          inFlight = Map.empty,
          isActive = false,
          lastRetryTime = timestamp,
          nextBackoff = retryConfig.exponentialBase,
          timerKillSwitch = None
        )
      )
  }

  /**
   * Accessor method for manually shutting down a managed server.
   */
  def shutdown: ZIO[Has[WebhookServer] with Clock, IOException, Any] =
    ZIO.service[WebhookServer].flatMap(_.shutdown) // serviceWith doesn't compile

  def subscribeToErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.subscribeToErrors)
}
