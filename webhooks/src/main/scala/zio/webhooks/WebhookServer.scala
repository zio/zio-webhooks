package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.json._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks.WebhookDeliveryBatching._
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer.WebhookState.Retrying
import zio.webhooks.WebhookServer._
import zio.webhooks.WebhookServerConfig.Batching
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
  private val internalState: SubscriptionRef[InternalState],
  private val batchingQueue: Option[Queue[(Webhook, WebhookEvent)]],
  private val changeQueue: Queue[WebhookState.Change],
  private val startupLatch: CountDownLatch,
  private val shutdownLatch: CountDownLatch
) {

  private def createRetry(dispatch: WebhookDispatch, timestamp: Instant) =
    Retry(dispatch, timestamp, config.retry.exponentialBase, config.retry.exponentialFactor)

  /**
   * Attempts delivery of a [[WebhookDispatch]] to the webhook receiver. On successful delivery,
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
      internalState.ref.update { internalState =>
        internalState.webhookState.get(id) match {
          case Some(WebhookState.Enabled)               =>
            changeToRetryState(id, internalState)
          case None                                     =>
            changeToRetryState(id, internalState)
          case Some(WebhookState.Retrying(_, queue, _)) =>
            queue.offer(dispatch) *> UIO(internalState)
          case Some(WebhookState.Disabled)              =>
            // TODO[review]: This can't happen as no
            UIO(internalState)
          case Some(WebhookState.Unavailable)           =>
            UIO(internalState)
        }
      }
    }

    for {
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      _        <- {
        (dispatch.deliverySemantics, response) match {
          case (_, Some(WebhookHttpResponse(200))) =>
            markDone(dispatch)
          case (AtLeastOnce, _)                    =>
            handleAtLeastOnce
          case (AtMostOnce, _)                     =>
            eventRepo.setEventStatusMany(dispatch.events.map(_.key), WebhookEventStatus.Failed)
        }
      }.catchAll(errorHub.publish)
    } yield ()
  }

  private def dispatchNewEvent(webhook: Webhook, event: WebhookEvent): ZIO[Clock, WebhookError, Unit] =
    for {
      _ <- eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivering)
      _ <- (webhook.batching, batchingQueue) match {
             case (Batched, Some(queue)) =>
               queue.offer((webhook, event.copy(status = WebhookEventStatus.Delivering)))
             case _                      =>
               deliver(WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, NonEmptyChunk(event)))
           }
    } yield ()

  private def doBatching(batchingQueue: Queue[(Webhook, WebhookEvent)], batching: Batching) = {
    val getWebhookIdAndContentType = (webhook: Webhook, event: WebhookEvent) =>
      (webhook.id, event.headers.find(_._1.toLowerCase == "content-type"))

    mergeShutdown(UStream.fromQueue(batchingQueue))
      .groupByKey(getWebhookIdAndContentType.tupled) {
        case (_, stream) =>
          stream
            .groupedWithin(batching.maxSize, batching.maxWaitTime)
            .map(NonEmptyChunk.fromChunk)
            .collectSome
            .mapM { elems =>
              val webhook  = elems.head._1
              val dispatch = WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, elems.map(_._2))
              deliver(dispatch)
            }
      }
      .runDrain *> shutdownLatch.countDown
  }

  private def doRetry(webhookId: WebhookId, retry: Retry, retryQueue: Queue[Retry]) = {
    val dispatch = retry.dispatch
    for {
      response  <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      nextState <- response match {
                     case Some(WebhookHttpResponse(200)) =>
                       markDone(dispatch) *>
                         internalState.ref.updateAndGet { state =>
                           UIO(state.removeRetry(webhookId, dispatch))
                         }
                     case _                              =>
                       for {
                         next  <- clock.instant.map(retry.next)
                         state <- internalState.ref.updateAndGet { state =>
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
   * Exposes a way to listen for [[WebhookError]]s, namely missing webhooks or events. This provides
   * clients a way to handle server errors that would otherwise just fail silently.
   */
  def getErrors: UManaged[Dequeue[WebhookError]] =
    errorHub.subscribe

  private def handleNewEvent(dequeue: Dequeue[WebhookEvent]) =
    for {
      raceResult <- dequeue.take raceEither isShutdown.takeUntil(identity).runDrain
      _          <- raceResult match {
                      case Left(newEvent) =>
                        val webhookId = newEvent.key.webhookId
                        for {
                          _ <- webhookRepo
                                 .getWebhookById(webhookId)
                                 .flatMap(ZIO.fromOption(_).orElseFail(MissingWebhookError(webhookId)))
                                 .flatMap(webhook => dispatchNewEvent(webhook, newEvent).when(webhook.isAvailable))
                                 .catchAll(errorHub.publish(_).unit)
                        } yield ()
                      case Right(_)       =>
                        ZIO.unit
                    }
      // TODO: replace with stream
      isShutdown <- internalState.ref.get.map(_.isShutdown)
    } yield isShutdown

  private def isShutdown                          =
    internalState.changes.collect { case InternalState(isShutdown, _) if isShutdown => isShutdown }

  private def markDone(dispatch: WebhookDispatch) =
    if (dispatch.size == 1)
      eventRepo.setEventStatus(dispatch.head.key, WebhookEventStatus.Delivered)
    else
      eventRepo.setEventStatusMany(dispatch.keys, WebhookEventStatus.Delivered)

  private def mergeShutdown[A](stream: UStream[A]) =
    stream
      .map(Left(_))
      .mergeTerminateRight(isShutdown.takeUntil(identity).map(Right(_)))
      .collectLeft

  // TODO: clean this up
  private def reconstructInternalState(loadedState: PersistentServerState): URIO[Clock, Unit] =
    for {
      eventMap   <- ZIO
                      .foreach(loadedState.map.keys.map(WebhookId(_))) { id =>
                        eventRepo
                          .getEventsByWebhookAndStatus(id, NonEmptySet(WebhookEventStatus.Delivering))
                          .map(dequeue => (id, dequeue))
                      }
                      .map(_.toMap)
      queues     <- ZIO.collectAll {
                      Chunk.fill(loadedState.map.size)(Queue.bounded[WebhookDispatch](config.retry.capacity))
                    }
      joinMap     = eventMap.map { case (id, events) => (id, events) }
                      .zip(queues)
                      .map {
                        case ((webhookId, events), dispatchQueue) =>
                          (webhookId, (events, dispatchQueue))
                      }
                      .toMap
                      .joinInner(loadedState.map.map { case (l, state) => (WebhookId(l), state) })
      timestamp  <- clock.instant
      retryStates = joinMap.map {
                      case (webhookId, ((events, dispatchQueue), loadedRetryingState)) =>
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
                        (webhookId, Retrying(loadedRetryingState.sinceTime, dispatchQueue, retryMap))
                    }
      _          <- internalState.ref.set(InternalState(isShutdown = false, retryStates))
      _          <- ZIO
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
      _ <- startBatching
      _ <- startupLatch.await
    } yield ()

  /**
   * Starts a fiber that listens to events queued for batched webhook dispatch.
   */
  private def startBatching =
    (config.batching, batchingQueue) match {
      case (Some(batching), Some(batchingQueue)) =>
        internalState.ref.get
          .map(_.isShutdown)
          .flatMap(ZIO.unless(_)(doBatching(batchingQueue, batching)).forkAs("batching"))
      case _                                     =>
        ZIO.unit
    }

  /**
   * Starts recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * [[WebhookDeliverySemantics.AtLeastOnce]]. Recovery is done by reconstructing
   * [[WebhookServer.InternalState]], the server's internal representation of webhooks it handles.
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery: URIO[Clock, Unit] =
    for {
      rawState <- stateRepo.getState
      _        <- ZIO.foreach_(rawState) { rawState =>
                    ZIO
                      .fromEither(rawState.fromJson[PersistentServerState])
                      .mapError(message => InvalidStateError(rawState, message))
                      .flatMap(reconstructInternalState)
                      .catchAll(errorHub.publish(_).unit)
                  }
    } yield ()

  /**
   * Starts new [[WebhookEvent]] subscription. Takes a latch which succeeds when the server is ready
   * to receive events.
   */
  private def startNewEventSubscription =
    eventRepo
      .getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .use { dequeue =>
        for {
          _          <- dequeue.poll
          _          <- startupLatch.countDown
          isShutdown <- internalState.ref.get.map(_.isShutdown)
          _          <- handleNewEvent(dequeue).repeatUntil(identity).unless(isShutdown)
          _          <- shutdownLatch.countDown
        } yield ()
      }
      .forkAs("new-event-subscription")

  /**
   * Starts retries on a webhook's dispatch queue. Retries until the retry map is exhausted.
   */
  private def startRetrying(webhookId: WebhookId, dispatchQueue: Queue[WebhookDispatch]) =
    for {
      retryQueue    <- Queue.bounded[Retry](config.retry.capacity)
      state         <- internalState.ref.get
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
                               internalState.ref
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
   * Performs retrying logic for webhooks. If reties time out, the webhook is set to
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
          _           <- internalState.ref.update { state =>
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
      _     <- internalState.ref.update(state => UIO(state.shutdown))
      _     <- shutdownLatch.await
      state <- internalState.ref.get.flatMap(toWebhookServerState)
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

  /**
   * Creates a server, pulling dependencies from the environment then initializing internal state.
   */
  def create: URIO[Env, WebhookServer] =
    for {
      serverConfig  <- ZIO.service[WebhookServerConfig]
      webhookRepo   <- ZIO.service[WebhookRepo]
      eventRepo     <- ZIO.service[WebhookEventRepo]
      httpClient    <- ZIO.service[WebhookHttpClient]
      webhookState  <- ZIO.service[WebhookStateRepo]
      state         <- SubscriptionRef.make(InternalState(isShutdown = false, Map.empty))
      errorHub      <- Hub.sliding[WebhookError](serverConfig.errorSlidingCapacity)
      batchingQueue <- ZIO
                         .foreach(serverConfig.batching) { batching =>
                           Queue.bounded[(Webhook, WebhookEvent)](batching.capacity)
                         }
      changeQueue   <- Queue.bounded[WebhookState.Change](serverConfig.retry.capacity)
      // startup sync point: new event sub
      startupLatch  <- CountDownLatch.make(1)
      // shutdown sync points: new event sub + retrying + optional batching
      latchCount     = 2 + serverConfig.batching.fold(0)(_ => 1)
      shutdownLatch <- CountDownLatch.make(latchCount)
    } yield new WebhookServer(
      webhookRepo,
      eventRepo,
      httpClient,
      serverConfig,
      webhookState,
      errorHub,
      state,
      batchingQueue,
      changeQueue,
      startupLatch,
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
  private[webhooks] final case class InternalState(isShutdown: Boolean, webhookState: Map[WebhookId, WebhookState]) {

    def removeRetry(id: WebhookId, dispatch: WebhookDispatch): InternalState =
      copy(webhookState = webhookState.updatedWithBackport(id)(_.map(_.removeRetry(dispatch))))

    def setRetry(id: WebhookId, retry: Retry): InternalState =
      copy(webhookState = webhookState.updatedWithBackport(id)(_.map(_.setRetry(retry))))

    def shutdown: InternalState = copy(isShutdown = true)

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
