package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.json._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks.PersistentServerState.RetryingState
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer.WebhookState.Retrying
import zio.webhooks.WebhookServer._
import zio.webhooks.internal.CountDownLatch

import java.io.IOException
import java.time.{ Instant, Duration => JDuration }

/**
 * A [[WebhookServer]] is a stateful server that subscribes to [[WebhookEvent]]s and reliably
 * delivers them, i.e. failed dispatches are retried once, followed by retries with exponential
 * backoff. Retries are performed until some duration after which webhooks will be marked
 * [[WebhookStatus.Unavailable]] since some [[java.time.Instant]]. Dispatches are batched iff a
 * `batchConfig` is defined ''and'' a webhook's delivery batching is
 * [[WebhookDeliveryBatching.Batched]].
 *
 * A live server layer is provided in the companion object for convenience and proper resource
 * management.
 */
final class WebhookServer private (
  private val webhookRepo: WebhookRepo,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val config: WebhookServerConfig,
  private val stateRepo: WebhookStateRepo,
  private val errorHub: Hub[WebhookError],
  private val internalState: RefM[InternalState],
  private val changeQueue: Queue[WebhookState.Change],
  private val startupLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val shutdownLatch: CountDownLatch
) {

  private def batchGroup(
    batchingCapacity: Int,
    batchQueues: RefM[Map[BatchKey, Queue[WebhookEvent]]],
    batchKey: BatchKey,
    batchEvents: ZStream[Any, Nothing, WebhookEvent]
  ) =
    for {
      webhook <- webhookRepo.requireWebhook(batchKey._1)
      _       <- webhook.deliveryMode.batching match {
                   case WebhookDeliveryBatching.Single  =>
                     batchEvents.foreach(deliverNewEvent)
                   case WebhookDeliveryBatching.Batched =>
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
                       _          <- doBatching(webhook, batchQueue, latch).fork
                       _          <- latch.await
                       _          <- batchEvents.run(ZSink.fromQueue(batchQueue))
                     } yield ()
                 }
    } yield ()

  private def createRetry(dispatch: WebhookDispatch, timestamp: Instant) =
    Retry(dispatch, timestamp, config.retry.exponentialBase, config.retry.exponentialFactor)

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, dispatches from webhooks with
   * at-least-once delivery semantics are enqueued for retrying.
   */
  private def deliver(dispatch: WebhookDispatch): URIO[Clock, Unit] = {
    def changeToRetryState(id: WebhookId, state: InternalState) =
      for {
        instant       <- clock.instant
        _             <- webhookRepo.setWebhookStatus(id, WebhookStatus.Retrying(instant))
        retryingState <- WebhookState.Retrying.make(config.retry.capacity)
        _             <- retryingState.dispatchQueue.offer(dispatch)
        _             <- changeQueue.offer(WebhookState.Change.ToRetrying(id, retryingState.dispatchQueue))
      } yield state.updateWebhookState(id, retryingState)

    def handleAtLeastOnce = {
      val id = dispatch.webhookId
      internalState.update { internalState =>
        internalState.webhookState.get(id) match {
          case Some(WebhookState.Enabled)               =>
            changeToRetryState(id, internalState)
          case None                                     =>
            changeToRetryState(id, internalState)
          case Some(WebhookState.Retrying(_, queue, _)) =>
            queue.offer(dispatch) *> UIO(internalState)
          case Some(WebhookState.Disabled)              =>
            // TODO: handle
            UIO(internalState)
          case Some(WebhookState.Unavailable)           =>
            // TODO: handle
            UIO(internalState)
        }
      }
    }

    for {
      _        <- markDispatch(dispatch, WebhookEventStatus.Delivering)
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      _        <- (dispatch.deliverySemantics, response) match {
                    case (_, Some(WebhookHttpResponse(200))) =>
                      markDispatch(dispatch, WebhookEventStatus.Delivered)
                    case (AtLeastOnce, _)                    =>
                      handleAtLeastOnce
                    case (AtMostOnce, _)                     =>
                      markDispatch(dispatch, WebhookEventStatus.Failed)
                  }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  private def deliverNewEvent(newEvent: WebhookEvent) = {
    for {
      webhook <- webhookRepo.requireWebhook(newEvent.key.webhookId)
      dispatch = WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, NonEmptyChunk(newEvent))
      _       <- deliver(dispatch).when(webhook.isAvailable).fork
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  private def doBatching(webhook: Webhook, events: Dequeue[WebhookEvent], latch: Promise[Nothing, Unit]) = {

    val batchingLoop = {
      for {
        batch   <- events.take.zipWith(events.takeAll)(NonEmptyChunk.fromIterable(_, _))
        dispatch = WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, batch)
        _       <- deliver(dispatch).when(webhook.isAvailable)
      } yield ()
    }
    events.poll *> latch.succeed(()) *> batchingLoop.forever
  }

  private def doRetry(webhookId: WebhookId, retry: Retry, retryQueue: Queue[Retry]) = {
    val dispatch = retry.dispatch
    for {
      response  <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      nextState <- response match {
                     case Some(WebhookHttpResponse(200)) =>
                       markDispatch(dispatch, WebhookEventStatus.Delivered) *>
                         internalState.updateAndGet { state =>
                           UIO(state.removeRetry(webhookId, dispatch))
                         }
                     case _                              =>
                       for {
                         next  <- clock.instant.map(retry.next)
                         state <- internalState.updateAndGet { state =>
                                    ZIO.foreach_(next.backoff) { backoff =>
                                      retryQueue.offer(next).delay(backoff).fork
                                    } *> UIO(state.setRetry(webhookId, next))
                                  }
                       } yield state
                   }

    } yield nextState.webhookState.get(webhookId).collect {
      case WebhookState.Retrying(_, _, retries) => retries
    }
  }

  /**
   * Exposes a way to listen for [[WebhookError]]s. This provides clients a way to handle server
   * errors that would otherwise just fail silently.
   */
  def getErrors: UManaged[Dequeue[WebhookError]] =
    errorHub.subscribe

  private def markDispatch(dispatch: WebhookDispatch, newStatus: WebhookEventStatus) =
    if (dispatch.size == 1)
      eventRepo.setEventStatus(dispatch.head.key, newStatus)
    else
      eventRepo.setEventStatusMany(dispatch.keys, newStatus)

  private def mergeShutdown[A](stream: UStream[A]) =
    stream
      .map(Left(_))
      .mergeTerminateRight(UStream.fromEffect(shutdownSignal.await.map(Right(_))))
      .collectLeft

  // TODO: clean this up
  /**
   * Reconstructs the server's internal retrying states from the loaded server state. Events are
   * loaded
   */
  private def reconstructInternalState(loadedState: PersistentServerState) =
    for {
      eventMap    <- eventRepo
                       .getEventsByStatuses(NonEmptySet(WebhookEventStatus.Delivering))
                       .use(_.takeAll.map(Chunk.fromIterable(_).groupBy(_.key.webhookId)))
      queues      <- ZIO.collectAll {
                       Chunk.fill(eventMap.size)(Queue.bounded[WebhookDispatch](config.retry.capacity))
                     }
      timestamp   <- clock.instant
      emptyStates  = Chunk.fill(eventMap.size)(RetryingState(timestamp, List.empty))
      idStateMap   = loadedState.map.map { case (l, state) => (WebhookId(l), state) }
      joinMap      = eventMap.map { case (id, events) => (id, events) }
                       .zip(emptyStates)
                       .zip(queues)
                       .map {
                         case (((webhookId, events), emptyState), dispatchQueue) =>
                           (webhookId, (events, dispatchQueue, idStateMap.getOrElse(webhookId, emptyState)))
                       }
                       .toMap
      retryStates <- ZIO.foreach(joinMap) {
                       case (webhookId, (events, dispatchQueue, loadedRetryingState)) =>
                         val dispatchKeys   = loadedRetryingState.retries
                           .map(_.dispatch.eventKeys)
                           .zipWithIndex
                           .map { case (set, i) => set.map((_, i)) }
                           .fold(Set.empty)(_.union(_))
                           .toMap
                         val retries        = Chunk.fromIterable(loadedRetryingState.retries)
                         val dispatchEvents = events
                           .filter(event => dispatchKeys.contains(event.key))
                           .foldLeft(Chunk.fill[Chunk[WebhookEvent]](loadedState.map.size)(Chunk.empty)) {
                             (dispatches, event) =>
                               dispatches.updated(
                                 dispatchKeys(event.key),
                                 dispatches(dispatchKeys(event.key)) :+ event
                               )
                           }
                           .map(NonEmptyChunk.fromChunk)
                           .collect { case Some(events) => events }
                         val retryMap       = retries
                           .zipWith(dispatchEvents) { (retry, dispatches) =>
                             val dispatch     =
                               WebhookDispatch(
                                 webhookId,
                                 retry.dispatch.url,
                                 retry.dispatch.deliverySemantics,
                                 dispatches
                               )
                             val retryMapElem = (
                               dispatch,
                               Retry(
                                 dispatch,
                                 timestamp,
                                 retry.base,
                                 retry.power,
                                 retry.backoff,
                                 retry.attempt
                               )
                             )
                             retryMapElem
                           }
                           .toMap

                         val enqueueUnsavedEvents =
                           webhookRepo
                             .requireWebhook(webhookId)
                             .flatMap { webhook =>
                               val dispatches = events
                                 .filterNot(event => dispatchKeys.contains(event.key))
                                 .map(event =>
                                   WebhookDispatch(
                                     webhookId,
                                     webhook.url,
                                     webhook.deliveryMode.semantics,
                                     NonEmptyChunk(event)
                                   )
                                 )
                                 .filter(_.deliverySemantics == WebhookDeliverySemantics.AtLeastOnce)
                               dispatchQueue.offerAll(dispatches)
                             }
                             .catchAll(errorHub.publish(_))
                             .fork
                         enqueueUnsavedEvents *>
                           UIO((webhookId, Retrying(loadedRetryingState.sinceTime, dispatchQueue, retryMap)))
                     }
      _           <- internalState.set(InternalState(retryStates))
      // put webhooks in retry state
      _           <- ZIO
                       .foreach_(retryStates) {
                         case (webhookId, retrying) =>
                           changeQueue.offer(WebhookState.Change.ToRetrying(webhookId, retrying.dispatchQueue))
                       }
                       .fork
    } yield ()

  /**
   * Starts the webhook server. The following are run concurrently:
   *
   *   - new webhook event subscription
   *   - event recovery for webhooks with at-least-once delivery semantics
   *   - dispatch retry monitoring
   *   - dispatch batching, if configured and enabled per webhook
   *
   * The server is ready once it signals readiness to accept new events.
   */
  def start: URIO[Clock, Any] =
    for {
      _ <- startEventRecovery
      _ <- startRetryMonitoring
      _ <- startNewEventSubscription
      _ <- startupLatch.await
    } yield ()

  private def startBatching(dequeue: Dequeue[WebhookEvent], batchingCapacity: Int) =
    for {
      batchQueues <- RefM.make(Map.empty[BatchKey, Queue[WebhookEvent]])
      _           <- mergeShutdown(UStream.fromQueue(dequeue))
                       .groupByKey(_.webhookIdAndContentType) {
                         case (batchKey, events) =>
                           ZStream.fromEffect {
                             batchGroup(batchingCapacity, batchQueues, batchKey, events)
                               .catchAll(errorHub.publish(_).unit)
                           }
                       }
                       .runDrain
    } yield ()

  /**
   * Starts recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * [[WebhookDeliverySemantics.AtLeastOnce]]. Recovery is done by reconstructing
   * [[WebhookServer.InternalState]], the server's internal representation of webhooks it handles.
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery: URIO[Clock, Unit] = {
    for {
      _ <- stateRepo.getState.flatMap {
             case Some(rawState) =>
               ZIO
                 .fromEither(rawState.fromJson[PersistentServerState])
                 .mapError(message => InvalidStateError(rawState, message))
                 .flatMap(reconstructInternalState)
             case None           =>
               reconstructInternalState(PersistentServerState.empty)
           }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  /**
   * Starts new [[WebhookEvent]] subscription. Counts down on the `startupLatch` signalling it's
   * ready to accept events.
   */
  private def startNewEventSubscription =
    eventRepo
      .getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .use { dequeue =>
        for {
          _           <- dequeue.poll
          _           <- startupLatch.countDown
          isShutdown  <- shutdownSignal.isDone
          handleEvents = config.batchingCapacity match {
                           case Some(capacity) =>
                             startBatching(dequeue, capacity)
                           case None           =>
                             mergeShutdown(UStream.fromQueue(dequeue)).foreach(deliverNewEvent)
                         }
          _           <- handleEvents.unless(isShutdown)
          _           <- shutdownLatch.countDown
        } yield ()
      }
      .forkAs("new-event-subscription")

  /**
   * Starts retries on a webhook's dispatch queue. Retries until the retry map is exhausted.
   */
  private def startRetrying(webhookId: WebhookId, dispatchQueue: Queue[WebhookDispatch]) =
    for {
      retryQueue    <- Queue.bounded[Retry](config.retry.capacity)
      state         <- internalState.get
      _             <- ZIO.collectAll_(state.webhookState.get(webhookId).collect {
                         case Retrying(_, _, retries) =>
                           ZIO.foreach_(retries.values) { retry =>
                             retry.backoff match {
                               case None          =>
                                 retryQueue.offer(retry)
                               case Some(backoff) =>
                                 retryQueue.offer(retry).delay(backoff).fork
                             }
                           }
                       })
      dispatchFiber <- mergeShutdown(UStream.fromQueue(dispatchQueue))
                         .mapM(dispatch =>
                           clock.instant.flatMap { timestamp =>
                             val retry = createRetry(dispatch, timestamp)
                             retryQueue.offer(retry) *>
                               // 2.12 fails to infer updateAndGet type params below
                               internalState
                                 .updateAndGet[Any, Nothing](state => UIO(state.setRetry(webhookId, retry)))
                           }
                         )
                         .runDrain
                         .fork
      retryFiber    <- mergeShutdown(UStream.fromQueue(retryQueue))
                         .mapM(retry => doRetry(webhookId, retry, retryQueue) zip dispatchQueue.size)
                         .takeUntil { case (retries, dispatchSize) => retries.exists(_.isEmpty) && dispatchSize <= 0 }
                         .runDrain
                         .fork
      _             <- retryFiber.join *> dispatchFiber.interrupt
    } yield ()

  /**
   * Performs retrying logic for webhooks. If retries time out, the webhook is set to
   * [[WebhookStatus.Unavailable]] and all its events are marked [[WebhookEventStatus.Failed]].
   */
  private def startRetryMonitoring = {
    mergeShutdown(UStream.fromQueue(changeQueue)).foreach {
      case WebhookState.Change.ToRetrying(id, dispatchQueue) =>
        (for {
          retriesDone <- startRetrying(id, dispatchQueue).timeoutTo(false)(_ => true)(config.retry.timeout)
          newStatus   <- if (retriesDone)
                           ZIO.succeed(WebhookStatus.Enabled)
                         else
                           eventRepo.setAllAsFailedByWebhookId(id) &> clock.instant.map(WebhookStatus.Unavailable)
          _           <- webhookRepo.setWebhookStatus(id, newStatus)
          _           <- internalState.update { state =>
                           UIO(state.updateWebhookState(id, WebhookState.from(newStatus)))
                         }
        } yield ()).catchAll(errorHub.publish).fork
    } *> shutdownLatch.countDown
  }.forkAs("retry-monitoring")

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: ZIO[Clock, IOException, Any] =
    for {
      _     <- shutdownSignal.succeed(())
      _     <- shutdownLatch.await
      state <- internalState.get.flatMap(toWebhookServerState)
      _     <- ZIO.foreach_(state)(state => stateRepo.setState(state.toJson))
    } yield ()

  /**
   * Maps the server's internal state into a [[PersistentServerState]] for persistence.
   */
  def toWebhookServerState(internalState: InternalState): URIO[Clock, Option[PersistentServerState]] =
    for {
      timestamp   <- clock.instant
      serverState <- if (internalState.webhookState.nonEmpty)
                       ZIO
                         .collectAll(internalState.webhookState.collect {
                           case (id, WebhookState.Retrying(sinceTime, dispatchQueue, retries)) =>
                             for {
                               moreRetries <- dispatchQueue.takeAll
                                                .map(_.map(dispatch => createRetry(dispatch, timestamp)))
                               allRetries   = (retries.values ++ moreRetries).map(_.suspend(timestamp))
                             } yield (
                               id.value,
                               PersistentServerState.RetryingState(
                                 sinceTime,
                                 allRetries.map(PersistentServerState.Retry.fromInternalState).toList
                               )
                             )
                         })
                         .map(webhookStates => Some(PersistentServerState(webhookStates.toMap)))
                     else
                       UIO.none
    } yield serverState
}

object WebhookServer {
  type BatchKey = (WebhookId, Option[(String, String)])

  /**
   * Creates a server, pulling dependencies from the environment then initializing internal state.
   */
  def create: URIO[Env, WebhookServer] =
    for {
      serverConfig   <- ZIO.service[WebhookServerConfig]
      webhookRepo    <- ZIO.service[WebhookRepo]
      eventRepo      <- ZIO.service[WebhookEventRepo]
      httpClient     <- ZIO.service[WebhookHttpClient]
      webhookState   <- ZIO.service[WebhookStateRepo]
      state          <- RefM.make(InternalState(Map.empty))
      errorHub       <- Hub.sliding[WebhookError](serverConfig.errorSlidingCapacity)
      changeQueue    <- Queue.bounded[WebhookState.Change](serverConfig.retry.capacity)
      // startup sync point: new event sub
      startupLatch   <- CountDownLatch.make(1)
      shutdownSignal <- Promise.make[Nothing, Unit]
      // shutdown sync points: new event sub + retrying
      shutdownLatch  <- CountDownLatch.make(2)
    } yield new WebhookServer(
      webhookRepo,
      eventRepo,
      httpClient,
      serverConfig,
      webhookState,
      errorHub,
      state,
      changeQueue,
      startupLatch,
      shutdownSignal,
      shutdownLatch
    )

  type Env = Has[WebhookRepo]
    with Has[WebhookStateRepo]
    with Has[WebhookEventRepo]
    with Has[WebhookHttpClient]
    with Has[WebhookServerConfig]
    with Clock

  def getErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.getErrors)

  /**
   * The server's [[InternalState]] is its shutdown state and the state of its webhooks. The server
   * uses its internal representation of each webhook's state to perform retrying logic. The
   * shutdown state is used as a signal to stop new event subscription, batching, and retrying.
   */
  private[webhooks] final case class InternalState(webhookState: Map[WebhookId, WebhookState]) {

    def removeRetry(id: WebhookId, dispatch: WebhookDispatch): InternalState =
      copy(webhookState = webhookState.updatedWithBackport(id)(_.map(_.removeRetry(dispatch))))

    def setRetry(id: WebhookId, retry: Retry): InternalState =
      copy(webhookState = webhookState.updatedWithBackport(id)(_.map(_.setRetry(retry))))

    def updateWebhookState(id: WebhookId, newWebhookState: WebhookState): InternalState =
      copy(webhookState = webhookState.updated(id, newWebhookState))
  }

  /**
   * Creates a server, ensuring shutdown on release.
   */
  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      server <- WebhookServer.create.toManaged_
      _      <- server.start.toManaged_
      _      <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server
  }.toLayer

  /**
   * A [[Retry]] represents the retry state of each dispatch. Retries back off exponentially.
   */
  private[webhooks] final case class Retry(
    dispatch: WebhookDispatch,
    timestamp: Instant,
    base: Duration,
    power: Double,
    backoff: Option[Duration] = None,
    attempt: Int = 0
  ) {

    /**
     * Progresses to the next retry by calculating the next exponential backoff.
     */
    def next(timestamp: Instant): Retry =
      copy(
        timestamp = timestamp,
        backoff = backoff.map(_ => Some(base * math.pow(2, attempt.toDouble))).getOrElse(Some(base)),
        attempt = attempt + 1
      )

    /**
     * Suspends this retry by replacing the backoff with the time left until its backoff completes.
     */
    def suspend(now: Instant): Retry =
      copy(backoff = backoff.map(_.minus(JDuration.between(now, timestamp))))
  }

  def shutdown: ZIO[Has[WebhookServer] with Clock, IOException, Any] =
    ZIO.environment[Has[WebhookServer] with Clock].flatMap(_.get[WebhookServer].shutdown)

  /**
   * [[WebhookState]] is the server's internal representation of a webhook's state.
   */
  private[webhooks] sealed trait WebhookState extends Product with Serializable { self =>
    final def setRetry(retry: Retry): WebhookState =
      self match {
        case retrying @ WebhookState.Retrying(_, _, retries) =>
          retrying.copy(retries = retries + (retry.dispatch -> retry))
        case _                                               =>
          self // should we fail here?
      }

    final def removeRetry(dispatch: WebhookDispatch): WebhookState =
      self match {
        case retrying @ WebhookState.Retrying(_, _, retries) =>
          retrying.copy(retries = retries - dispatch)
        case _                                               =>
          self // should we fail here?
      }
  }

  private[webhooks] object WebhookState {
    sealed trait Change
    object Change {
      final case class ToRetrying(id: WebhookId, queue: Queue[WebhookDispatch]) extends Change
    }

    case object Disabled extends WebhookState

    case object Enabled extends WebhookState

    val from: PartialFunction[WebhookStatus, WebhookState] = {
      case WebhookStatus.Enabled        => WebhookState.Enabled
      case WebhookStatus.Disabled       => WebhookState.Disabled
      case WebhookStatus.Unavailable(_) => WebhookState.Unavailable
    }

    final case class Retrying(
      sinceTime: Instant,
      dispatchQueue: Queue[WebhookDispatch],
      retries: Map[WebhookDispatch, Retry] = Map.empty
    ) extends WebhookState

    object Retrying {
      def make(capacity: Int): URIO[Clock, Retrying] =
        ZIO.mapN(clock.instant, Queue.bounded[WebhookDispatch](capacity))(Retrying(_, _))
    }

    case object Unavailable extends WebhookState
  }
}
