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
  private val config: WebhookServerConfig,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val stateRepo: WebhookStateRepo,
  private val errorHub: Hub[WebhookError],
  private val newRetries: Queue[NewRetry],
  private val permits: Semaphore,
  private val retries: RefM[Retries],
  private val startupLatch: CountDownLatch,
  private val shutdownLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhooks: WebhooksProxy
) {

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook's endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, events delivered to
   * at-least-once webhooks with are enqueued for retrying, while dispatches to at-most-once
   * webhooks are marked failed.
   */
  private def deliver(dispatch: WebhookDispatch): URIO[Clock, Unit] = {
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
                        retries.map.get(webhookId) match {
                          // we're already retrying. add events to a retry queue
                          case Some(retryState) =>
                            for {
                              retryState <- retryState.activateWithTimeout(markWebhookUnavailable(webhookId))
                              _          <- retryState.enqueueAll(dispatch.events)
                            } yield retries.updateRetryState(webhookId, retryState)
                          // start retrying, add events to the retry queue
                          case _                =>
                            for {
                              retryState <- retries.map
                                              .get(webhookId)
                                              .fold(RetryState.make(config.retry))(UIO(_))
                              retryState <- retryState.activateWithTimeout(markWebhookUnavailable(webhookId))
                              _          <- retryState.enqueueAll(dispatch.events)
                              _          <- newRetries.offer(NewRetry(webhookId, retryState))
                            } yield retries.updateRetryState(webhookId, retryState)
                        }
                      }
                  }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  private def deliverNewEvent(newEvent: WebhookEvent): URIO[Clock, Unit] = {
    for {
      webhook <- webhooks.getWebhookById(newEvent.key.webhookId)
      dispatch = WebhookDispatch(
                   webhook.id,
                   webhook.url,
                   webhook.deliveryMode.semantics,
                   NonEmptySet(newEvent)
                 )
      _       <- deliver(dispatch).when(webhook.isAvailable)
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  /**
   * Runs an infinite loop that takes all the current elements in a batched retry queue then
   * delivers them.
   */
  private def doRetryBatching(
    batchQueue: Queue[WebhookEvent],
    latch: Promise[Nothing, Unit],
    retryState: Ref[RetryState]
  ): ZIO[Clock, WebhookError, Nothing] = {
    val deliverBatch =
      for {
        batch    <- batchQueue.take.zipWith(batchQueue.takeAll)(NonEmptySet.fromIterable(_, _))
        webhookId = batch.head.key.webhookId
        webhook  <- webhooks.getWebhookById(webhookId)
        _        <- retryEvents(retryState, batch, Some(batchQueue)).when(webhook.isAvailable)
      } yield ()
    batchQueue.poll *> latch.succeed(()) *> deliverBatch.forever
  }

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
  private def markWebhookUnavailable(webhookId: WebhookId): ZIO[Clock, WebhookError, Unit] =
    for {
      _                 <- eventRepo.setAllAsFailedByWebhookId(webhookId)
      unavailableStatus <- clock.instant.map(WebhookStatus.Unavailable)
      _                 <- webhooks.setWebhookStatus(webhookId, unavailableStatus)
    } yield ()

  /**
   * Loads the persisted retry states and resumes them.
   */
  private def loadRetries(loadedState: PersistentRetries): ZIO[Clock, WebhookError, Unit] =
    for {
      retryMap <- ZIO.foreach(loadedState.retryStates) {
                    case (id, persistedState) =>
                      resumeRetrying(WebhookId(id), persistedState).map((WebhookId(id), _))
                  }
      _        <- retries.set(Retries(retryMap))
      _        <- ZIO.foreach_(retryMap) {
                    case (webhookId, retryState) =>
                      newRetries.offer(NewRetry(webhookId, retryState))
                  }
    } yield ()

  /**
   * Recovers an event by adding it to a retry queue, or starting retry dispatch for a webhook if
   * retries for it haven't started yet.
   */
  private def recoverEvent(event: WebhookEvent): URIO[Clock, Unit] =
    for {
      retryQueue <- retries.modify { state =>
                      state.map.get(event.key.webhookId) match {
                        // we're continuing retries for this webhook
                        case Some(retryState) =>
                          UIO((Some(retryState.retryQueue), state))
                        // no retry state was loaded for this webhook, make a new one
                        case None             =>
                          RetryState.make(config.retry).map { retryState =>
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
  ): ZIO[Clock, WebhookError, RetryState] =
    for {
      loadedState  <- ZIO.mapN(
                        Queue.bounded[WebhookEvent](config.retry.capacity),
                        Queue.bounded[Promise[Nothing, Unit]](config.retry.capacity)
                      ) { (retryQueue, backoffResets) =>
                        RetryState(
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
   * Performs batched retries. Batching works similarly to regular retries.
   */
  def retryBatched(retryState: Ref[RetryState], webhookId: WebhookId, batchingCapacity: Int): URIO[Clock, Unit] =
    for {
      batchQueues <- RefM.make(Map.empty[BatchKey, Queue[WebhookEvent]])
      retryQueue  <- retryState.get.map(_.retryQueue)
      _           <- mergeShutdown(UStream.fromQueue(retryQueue), shutdownSignal)
                       .groupByKey(ev => BatchKey(webhookId, ev.contentType)) {
                         case (batchKey, batchEvents) =>
                           ZStream.fromEffect {
                             for {
                               batchQueue <- batchQueues.modify { map =>
                                               map.get(batchKey) match {
                                                 case Some(queue) =>
                                                   UIO((queue, map))
                                                 case None        =>
                                                   for (queue <- Queue.bounded[WebhookEvent](batchingCapacity))
                                                     yield (queue, map + (batchKey -> queue))
                                               }
                                             }
                               latch      <- Promise.make[Nothing, Unit]
                               _          <- doRetryBatching(
                                               batchQueue,
                                               latch,
                                               retryState
                                             ).fork
                               _          <- latch.await
                               _          <- batchEvents.run(ZSink.fromQueue(batchQueue))
                             } yield ()
                           }
                       }
                       .runDrain
                       .fork
    } yield ()

  /**
   * Attempts to retry a non-empty chunk of events. Each attempt updates the retry state based on
   * the outcome of the attempt.
   *
   * Each failed attempt causes the retry backoff to increase exponentially, so as not to flood the
   * endpoint with retry attempts.
   *
   * On the other hand, each successful attempt resets the backoff—allowing for greater throughput
   * for retries when the endpoint begins to return `200` status codes.
   */
  private def retryEvents(
    retryState: Ref[RetryState],
    events: NonEmptySet[WebhookEvent],
    batchQueue: Option[Queue[WebhookEvent]] = None
  ): ZIO[Clock, WebhookError, Unit] = {
    val webhookId = events.head.key.webhookId
    for {
      webhook  <- webhooks.getWebhookById(webhookId)
      _        <- retryState.update(_.addInFlight(events))
      dispatch  = WebhookDispatch(
                    webhook.id,
                    webhook.url,
                    webhook.deliveryMode.semantics,
                    events
                  )
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).either
      _        <- response match {
                    case Left(Left(badWebhookUrlError))  =>
                      errorHub.publish(badWebhookUrlError)
                    case Right(WebhookHttpResponse(200)) =>
                      for {
                        _                <- retryState.update(_.removeInFlight(events))
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
                                                retries.updateRetryState(webhookId, newState)
                                              )
                                            }
                        queueEmpty       <- newState.retryQueue.size.map(_ <= 0)
                        batchExistsEmpty <- ZIO.foreach(batchQueue)(_.size.map(_ <= 0))
                        inFlightEmpty    <- retryState.get.map(_.inFlight.isEmpty)
                        allEmpty          = queueEmpty && inFlightEmpty && batchExistsEmpty.getOrElse(true)
                        setInactive       = retryState.get.flatMap(_.deactivate)
                        _                <- setInactive.when(allEmpty)
                      } yield ()
                    // retry responded with a non-200 status, or an IOException occurred
                    case _                               =>
                      for {
                        timestamp <- clock.instant
                        nextState <- retryState.updateAndGet(_.increaseBackoff(timestamp))
                        _         <- retries.update(state => UIO(state.updateRetryState(webhookId, nextState)))
                        requeue    = nextState.requeue(events) *> retryState.update(_.removeInFlight(events))
                        // prevent batches from getting into deadlocks by forking the requeue
                        _         <- if (batchQueue.isDefined) requeue.fork else requeue
                      } yield ()
                  }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  /**
   * Retries events one-by-one asynchronously, taking events and
   */
  private def retrySingly(retryState: Ref[RetryState]): ZIO[Clock, Nothing, Unit] =
    for {
      retryQueue <- retryState.get.map(_.retryQueue)
      _          <- mergeShutdown(UStream.fromQueue(retryQueue), shutdownSignal).foreach { event =>
                      permits.withPermit(retryEvents(retryState, NonEmptySet.single(event))).fork
                    }.fork
    } yield ()

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
  def start: URIO[Clock, Any] =
    for {
      _ <- startEventRecovery
      _ <- startRetryMonitoring
      _ <- startNewEventSubscription
      _ <- startWebhookSubscription
      _ <- startupLatch.await
    } yield ()

  /**
   * Starts the recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * at-least-once delivery semantics. Loads [[WebhookServer.Retries]] then enqueues events into the
   * retry queue for its webhook.
   *
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery: URIO[Clock, Any] = {
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
               webhook <- webhooks.getWebhookById(event.key.webhookId)
               _       <- recoverEvent(event).when(webhook.isAvailable)
             } yield ()).catchAll(errorHub.publish)
           }
    } yield ()
  }.fork *> startupLatch.countDown

  /**
   * Starts server subscription to new [[WebhookEvent]]s. Counts down on the `startupLatch`,
   * signalling that it's ready to accept new events.
   */
  private def startNewEventSubscription: URIO[Clock, Any] =
    eventRepo.subscribeToNewEvents.use { eventDequeue =>
      for {
        // signal that the server is ready to accept new webhook events
        _               <- eventDequeue.poll *> startupLatch.countDown
        isShutdown      <- shutdownSignal.isDone
        deliverFunc     <- ZIO
                             .environment[Clock]
                             .map(clock => (dispatch: WebhookDispatch) => deliver(dispatch).provide(clock))
        batchDispatcher <- ZIO.foreach(config.batchingCapacity)(
                             BatchDispatcher
                               .create(_, deliverFunc, errorHub, shutdownSignal, webhooks)
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

  private def handleNewEvent(batchDispatcher: Option[BatchDispatcher], event: WebhookEvent) =
    for {
      webhook <- webhooks.getWebhookById(event.key.webhookId)
      _       <- ((batchDispatcher, webhook.batching) match {
                     case (Some(batchDispatcher), WebhookDeliveryBatching.Batched) =>
                       batchDispatcher.enqueueEvent(event)
                     case _                                                        =>
                       permits.withPermit(deliverNewEvent(event)).fork
                   }).when(webhook.isAvailable)
    } yield ()

  /**
   * Listens for new retries and starts retry dispatching for a webhook.
   */
  private def startRetryMonitoring: URIO[Clock, Any] = {
    mergeShutdown(UStream.fromQueue(newRetries), shutdownSignal).foreach {
      case NewRetry(webhookId, retryState) =>
        (for {
          retryState <- Ref.make(retryState)
          webhook    <- webhooks.getWebhookById(webhookId)
          _          <- (webhook.batching, config.batchingCapacity) match {
                          case (WebhookDeliveryBatching.Batched, Some(capacity)) =>
                            retryBatched(retryState, webhookId, capacity)
                          case _                                                 =>
                            retrySingly(retryState)
                        }
        } yield ()).catchAll(errorHub.publish)
    } *> shutdownLatch.countDown
  }.fork

  def startWebhookSubscription: URIO[Clock, Any] = ZIO.unit

  /**
   * Waits until all work in progress is finished, persists retries, then shuts down.
   */
  def shutdown: URIO[Clock, Any] =
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
      config           <- ZIO.service[WebhookServerConfig]
      eventRepo        <- ZIO.service[WebhookEventRepo]
      httpClient       <- ZIO.service[WebhookHttpClient]
      webhookState     <- ZIO.service[WebhookStateRepo]
      webhooks         <- ZIO.service[WebhooksProxy]
      errorHub         <- Hub.sliding[WebhookError](config.errorSlidingCapacity)
      newRetries       <- Queue.bounded[NewRetry](config.retry.capacity)
      singleDispatches <- Semaphore.make(config.maxSingleDispatchConcurrency.toLong)
      retries          <- RefM.make(Retries(Map.empty))
      // startup sync points: new event sub + event recovery
      startupLatch     <- CountDownLatch.make(2)
      // shutdown sync points: new event sub + event recovery + retrying
      shutdownLatch    <- CountDownLatch.make(2)
      shutdownSignal   <- Promise.make[Nothing, Unit]
    } yield new WebhookServer(
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
  val live: URLayer[WebhookServer.Env with Clock, Has[WebhookServer]] = {
    for {
      server <- WebhookServer.create.toManaged_
      _      <- server.start.toManaged_
      _      <- ZManaged.finalizer(server.shutdown)
    } yield server
  }.toLayer

  /**
   * A [[NewRetry]] signals the first time a delivery has failed for a webhook with at-least-onc
   * delivery semantics and that the server should begin retrying events.
   */
  private[webhooks] final case class NewRetry(id: WebhookId, retryState: RetryState)

  /**
   * A [[RetryState]] represents retrying logic for a single webhook.
   */
  private[webhooks] final case class RetryState private (
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
    def activateWithTimeout[R, E](onTimeout: ZIO[R, E, Unit]): ZIO[R with Clock, E, RetryState] =
      if (isActive)
        UIO(this)
      else
        for {
          timerKillSwitch <- Promise.make[Nothing, Unit]
          runTimer         = timerKillSwitch.await
                               .timeoutTo(false)(_ => true)(timeoutDuration)
                               .flatMap(onTimeout.unless(_))
          _               <- runTimer.fork
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

    def requeue(events: NonEmptySet[WebhookEvent]): URIO[Clock, Unit] =
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
    def make(retryConfig: WebhookServerConfig.Retry): URIO[Clock, RetryState] =
      ZIO.mapN(
        Queue.bounded[WebhookEvent](retryConfig.capacity),
        Queue.bounded[Promise[Nothing, Unit]](retryConfig.capacity),
        clock.instant
      )((retryQueue, backoffResetsQueue, timestamp) =>
        RetryState(
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
