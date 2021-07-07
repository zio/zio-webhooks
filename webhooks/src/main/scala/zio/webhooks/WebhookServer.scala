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
  private val config: WebhookServerConfig,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val stateRepo: WebhookStateRepo,
  private val webhookRepo: WebhookRepo,
  private val changeQueue: Queue[WebhookState.IncomingChange],
  private val errorHub: Hub[WebhookError],
  private val internalState: RefM[InternalState],
  private val startupLatch: CountDownLatch,
  private val shutdownLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit]
) {

  private def batchGroup(
    batchingCapacity: Int,
    batchQueues: RefM[Map[BatchKey, Queue[WebhookEvent]]],
    batchKey: BatchKey,
    batchEvents: UStream[WebhookEvent]
  ) =
    for {
      webhook <- webhookRepo.requireWebhook(batchKey.webhookId)
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

  private def batchGroupRetry(
    batchingCapacity: Int,
    batchQueues: RefM[Map[BatchKey, Queue[WebhookEvent]]],
    batchKey: BatchKey,
    batchEvents: ZStream[Any, Nothing, WebhookEvent],
    retryingState: Ref[WebhookState.Retrying],
    retryingDone: Promise[Nothing, Unit]
  ) =
    for {
      webhook    <- webhookRepo.requireWebhook(batchKey.webhookId)
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
      _          <- doRetryBatching(webhook, batchQueue, latch, retryingState, retryingDone).fork
      _          <- latch.await
      _          <- batchEvents.run(ZSink.fromQueue(batchQueue))
    } yield ()

  private def createRetryingState(timestamp: Instant) =
    Queue
      .bounded[WebhookEvent](config.retry.capacity)
      .map(retryQueue =>
        WebhookState.Retrying(
          sinceTime = timestamp,
          retryQueue,
          lastRetryTime = timestamp,
          config.retry.exponentialBase,
          config.retry.exponentialFactor,
          timeLeft = config.retry.timeout,
          nextBackoff = config.retry.exponentialBase
        )
      )

  /**
   * Attempts delivery of a [[WebhookDispatch]] to a webhook endpoint. On successful delivery,
   * events are marked [[WebhookEventStatus.Delivered]]. On failure, dispatches from webhooks with
   * at-least-once delivery semantics are enqueued for retrying.
   */
  private def deliver(dispatch: WebhookDispatch): URIO[Clock, Unit] = {
    def changeToRetryingState(id: WebhookId, state: InternalState) =
      for {
        instant       <- clock.instant
        _             <- webhookRepo.setWebhookStatus(id, WebhookStatus.Retrying(instant))
        retryingState <- createRetryingState(instant)
        _             <- retryingState.enqueueAll(dispatch.events)
        _             <- changeQueue.offer(WebhookState.IncomingChange.ToRetrying(id, retryingState))
      } yield state.updateWebhookState(id, retryingState)

    def handleAtLeastOnce = {
      val id = dispatch.webhookId
      internalState.update { internalState =>
        internalState.webhookState.get(id) match {
          case Some(WebhookState.Enabled)            =>
            changeToRetryingState(id, internalState)
          case None                                  =>
            changeToRetryingState(id, internalState)
          case Some(retrying: WebhookState.Retrying) =>
            retrying.enqueueAll(dispatch.events) *> UIO(internalState)
          case Some(WebhookState.Disabled)           =>
            // TODO: handle
            UIO(internalState)
          case Some(WebhookState.Unavailable)        =>
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

  private def doBatching(webhook: Webhook, batchQueue: Dequeue[WebhookEvent], latch: Promise[Nothing, Unit]) = {
    val deliverBatch = for {
      batch   <- batchQueue.take.zipWith(batchQueue.takeAll)(NonEmptyChunk.fromIterable(_, _))
      dispatch = WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, batch)
      _       <- deliver(dispatch).when(webhook.isAvailable)
    } yield ()
    batchQueue.poll *> latch.succeed(()) *> deliverBatch.forever
  }

  private def doRetryBatching(
    webhook: Webhook,
    batchQueue: Queue[WebhookEvent],
    latch: Promise[Nothing, Unit],
    retryingState: Ref[WebhookState.Retrying],
    retryingDone: Promise[Nothing, Unit]
  ) = {
    val deliverBatch = for {
      batchEvents <- batchQueue.take.zipWith(batchQueue.takeAll)(NonEmptyChunk.fromIterable(_, _))
      _           <- retryEvents(webhook.id, retryingState, retryingDone, batchEvents)
    } yield ()
    batchQueue.poll *> latch.succeed(()) *> deliverBatch.repeatUntilM(_ => retryingDone.isDone)
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

  /**
   * Merges a stream with the shutdown signal, terminating it when the signal arrives.
   */
  private def mergeShutdown[A](stream: UStream[A]): UStream[A] =
    stream
      .map(Left(_))
      .mergeTerminateRight(UStream.fromEffect(shutdownSignal.await.map(Right(_))))
      .collectLeft

  /**
   * Reconstructs the server's internal retrying states from the loaded server state.
   */
  private def reconstructInternalState(loadedState: PersistentServerState): ZIO[Clock, Nothing, Unit] =
    for {
      retryingMap <- ZIO.foreach(loadedState.retryingStates) {
                       case (id, retryingState) =>
                         for (retryQueue <- Queue.bounded[WebhookEvent](config.retry.capacity))
                           yield (
                             WebhookId(id),
                             WebhookState.Retrying(
                               retryingState.sinceTime,
                               retryQueue,
                               retryingState.lastRetryTime,
                               retryingState.base,
                               retryingState.power,
                               retryingState.timeLeft,
                               retryingState.backoff,
                               retryingState.attempt
                             )
                           )

                     }
      _           <- internalState.set(InternalState(retryingMap))
      _           <- ZIO.foreach_(retryingMap) {
                       case (webhookId, retryingState) =>
                         changeQueue.offer(WebhookState.IncomingChange.ToRetrying(webhookId, retryingState))
                     }
    } yield ()

  def retryBatched(
    retryingState: Ref[WebhookState.Retrying],
    webhookId: WebhookId,
    batchingCapacity: Int,
    retryingDone: Promise[Nothing, Unit]
  ): URIO[Clock, Unit] =
    for {
      batchQueues <- RefM.make(Map.empty[BatchKey, Queue[WebhookEvent]])
      retryQueue  <- retryingState.get.map(_.retryQueue)
      _           <- mergeShutdown(UStream.fromQueue(retryQueue))
                       .groupByKey(ev => BatchKey(webhookId, ev.contentType)) {
                         case (batchKey, batchEvents) =>
                           ZStream.fromEffect {
                             batchGroupRetry(
                               batchingCapacity,
                               batchQueues,
                               batchKey,
                               batchEvents,
                               retryingState,
                               retryingDone
                             ).catchAll(errorHub.publish(_).unit)
                           }
                       }
                       .runDrain
                       .fork
    } yield ()

  private def retryEvents(
    webhookId: WebhookId,
    retryingState: Ref[WebhookState.Retrying],
    retryingDone: Promise[Nothing, Unit],
    events: NonEmptyChunk[WebhookEvent]
  ) =
    for {
      webhook  <- webhookRepo.requireWebhook(webhookId)
      _        <- retryingState.update(_.addInFlight(events))
      dispatch  = WebhookDispatch(
                    webhook.id,
                    webhook.url,
                    webhook.deliveryMode.semantics,
                    events
                  )
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      _        <- response match {
                    case Some(WebhookHttpResponse(200)) =>
                      for {
                        _             <- retryingState.update(_.removeInFlight(events))
                        _             <- markDispatch(dispatch, WebhookEventStatus.Delivered)
                        now           <- clock.instant
                        newState      <- internalState.modify { internalState =>
                                           for (newState <- retryingState.updateAndGet(_.resetBackoff(now)))
                                             yield (
                                               newState,
                                               internalState.updateWebhookState(webhookId, newState)
                                             )
                                         }
                        queueEmpty    <- newState.retryQueue.size.map(_ <= 0)
                        inFlightEmpty <- retryingState.get.map(_.inFlight.isEmpty)
                        _             <- ZIO.when(queueEmpty && inFlightEmpty)(
                                           newState.retryQueue.shutdown *> retryingDone.succeed(())
                                         )
                      } yield ()
                    case _                              =>
                      for {
                        timestamp <- clock.instant
                        nextState <- retryingState.updateAndGet(_.increaseBackoff(timestamp))
                        _         <- internalState.update(state => UIO(state.updateWebhookState(webhookId, nextState)))
                        _         <- nextState.requeue(events) *> retryingState.update(_.removeInFlight(events))
                      } yield ()
                  }
    } yield ()

  private def retrySingly(
    retryingState: Ref[WebhookState.Retrying],
    webhookId: WebhookId,
    retryingDone: Promise[Nothing, Unit]
  ) =
    for {
      retryQueue <- retryingState.get.map(_.retryQueue)
      _          <- mergeShutdown(UStream.fromQueue(retryQueue))
                      .mapM(event =>
                        retryEvents(
                          webhookId,
                          retryingState,
                          retryingDone,
                          NonEmptyChunk.single(event)
                        ).catchAll(errorHub.publish).fork
                      )
                      .runDrain
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
      _           <- mergeShutdown(UStream.fromQueue(dequeue)).groupByKey { ev =>
                       val (webhookId, contentType) = ev.webhookIdAndContentType
                       BatchKey(webhookId, contentType)
                     } {
                       case (batchKey, events) =>
                         ZStream.fromEffect {
                           batchGroup(batchingCapacity, batchQueues, batchKey, events)
                             .catchAll(errorHub.publish(_).unit)
                         }
                     }.runDrain
    } yield ()

  /**
   * Starts recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * delivery semantics [[WebhookDeliverySemantics.AtLeastOnce]]. Recovery is done by reconstructing
   * [[WebhookServer.InternalState]], the server's internal representation of webhooks it handles.
   * Events are loaded incrementally and are queued for retrying.
   *
   * This ensures retries are persistent with respect to server restarts.
   */
  private def startEventRecovery: URIO[Clock, Any] = {
    def recover(event: WebhookEvent) =
      for {
        retryQueue <- internalState.modify { state =>
                        state.webhookState.get(event.key.webhookId) match {
                          case Some(retrying: WebhookState.Retrying) =>
                            UIO((Some(retrying.retryQueue), state))
                          case None                                  =>
                            ZIO.mapN(Queue.bounded[WebhookEvent](config.retry.capacity), clock.instant) {
                              (retryQueue, now) =>
                                (
                                  Some(retryQueue),
                                  state.updateWebhookState(
                                    event.key.webhookId,
                                    WebhookState.Retrying(
                                      sinceTime = now,
                                      retryQueue,
                                      lastRetryTime = now,
                                      base = config.retry.exponentialBase,
                                      power = config.retry.exponentialFactor,
                                      timeLeft = config.retry.timeout,
                                      nextBackoff = config.retry.exponentialBase
                                    )
                                  )
                                )
                            }
                          case _                                     =>
                            UIO((None, state))
                        }
                      }
        _          <- ZIO.foreach_(retryQueue)(_.offer(event))
      } yield ()

    {
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
        _ <- eventRepo.getEventsByStatuses(NonEmptySet.single(WebhookEventStatus.Delivering)).use { dequeue =>
               dequeue.poll *> startupLatch.countDown *>
                 UStream.fromQueue(dequeue).foreach { event =>
                   for {
                     webhook <- webhookRepo.requireWebhook(event.key.webhookId)
                     _       <- recover(event).when(webhook.isAvailable)
                   } yield ()
                 }
             }
      } yield ()
    }.catchAll(errorHub.publish(_).unit).fork
  }

  /**
   * Starts new [[WebhookEvent]] subscription. Counts down on the `startupLatch` signalling it's
   * ready to accept events.
   */
  private def startNewEventSubscription =
    eventRepo
      .getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .use { eventDequeue =>
        for {
          _           <- eventDequeue.poll *> startupLatch.countDown
          isShutdown  <- shutdownSignal.isDone
          handleEvents = config.batchingCapacity match {
                           case Some(capacity) =>
                             startBatching(eventDequeue, capacity)
                           case None           =>
                             mergeShutdown(UStream.fromQueue(eventDequeue)).foreach(deliverNewEvent)
                         }
          _           <- handleEvents.unless(isShutdown)
          _           <- shutdownLatch.countDown
        } yield ()
      }
      .fork

  /**
   * Starts retries on a webhook's retry queue. Retries until the retry map is exhausted.
   */
  private def startRetrying(
    webhookId: WebhookId,
    retryingState: WebhookState.Retrying
  ): ZIO[Clock, WebhookError, Unit] =
    for {
      retryingState <- Ref.make(retryingState)
      webhook       <- webhookRepo.requireWebhook(webhookId)
      retryingDone  <- Promise.make[Nothing, Unit]
      _             <- (webhook.batching, config.batchingCapacity) match {
                         case (WebhookDeliveryBatching.Batched, Some(capacity)) =>
                           retryBatched(retryingState, webhookId, capacity, retryingDone)
                         case _                                                 =>
                           retrySingly(retryingState, webhookId, retryingDone)
                       }
      _             <- retryingDone.await
    } yield ()

  /**
   * Performs retrying logic for webhooks. If retrying time out, the webhook is set to
   * [[WebhookStatus.Unavailable]] and all its events are marked [[WebhookEventStatus.Failed]].
   */
  private def startRetryMonitoring: URIO[Clock, Any] = {
    mergeShutdown(UStream.fromQueue(changeQueue)).foreach {
      case WebhookState.IncomingChange.ToRetrying(id, retrying) =>
        (for {
          doneInTime <- startRetrying(id, retrying).timeoutTo(false)(_ => true)(retrying.timeLeft)
          newStatus  <- if (doneInTime)
                          UIO(WebhookStatus.Enabled)
                        else
                          eventRepo.setAllAsFailedByWebhookId(id) *> clock.instant.map(WebhookStatus.Unavailable)
          _          <- webhookRepo.setWebhookStatus(id, newStatus)
          _          <- internalState.update { state =>
                          UIO(state.updateWebhookState(id, WebhookState.from(newStatus)))
                        }
        } yield ()).catchAll(errorHub.publish).fork
    } *> shutdownLatch.countDown
  }.fork

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: ZIO[Clock, IOException, Any] =
    for {
      _               <- shutdownSignal.succeed(())
      _               <- shutdownLatch.await
      timestamp       <- clock.instant
      persistentState <- internalState.modify { internalState =>
                           val suspendedState = internalState.suspendRetries(timestamp)
                           UIO((toPersistentServerState(suspendedState), suspendedState))
                         }
      _               <- stateRepo.setState(persistentState.toJson)
    } yield ()

  /**
   * Maps the server's internal state into a [[PersistentServerState]].
   */
  private def toPersistentServerState(internalState: InternalState): PersistentServerState =
    PersistentServerState(internalState.webhookState.collect {
      case (webhookId, retrying: WebhookState.Retrying) =>
        val retryingState = PersistentServerState.RetryingState(
          sinceTime = retrying.sinceTime,
          lastRetryTime = retrying.lastRetryTime,
          base = retrying.base,
          power = retrying.power,
          timeLeft = retrying.timeLeft,
          backoff = retrying.nextBackoff,
          attempt = retrying.attempt
        )
        (webhookId.value, retryingState)
    })
}

object WebhookServer {
  final case class BatchKey(webhookId: WebhookId, contentType: Option[String])

  /**
   * Creates a server, pulling dependencies from the environment then initializing internal state.
   */
  def create: URIO[Env, WebhookServer] =
    for {
      config         <- ZIO.service[WebhookServerConfig]
      eventRepo      <- ZIO.service[WebhookEventRepo]
      httpClient     <- ZIO.service[WebhookHttpClient]
      webhookRepo    <- ZIO.service[WebhookRepo]
      webhookState   <- ZIO.service[WebhookStateRepo]
      changeQueue    <- Queue.bounded[WebhookState.IncomingChange](config.retry.capacity)
      errorHub       <- Hub.sliding[WebhookError](config.errorSlidingCapacity)
      state          <- RefM.make(InternalState(Map.empty))
      // startup sync points: new event sub + event recovery
      startupLatch   <- CountDownLatch.make(2)
      // shutdown sync points: new event sub + event recovery + retrying
      shutdownLatch  <- CountDownLatch.make(2)
      shutdownSignal <- Promise.make[Nothing, Unit]
    } yield new WebhookServer(
      config,
      eventRepo,
      httpClient,
      webhookState,
      webhookRepo,
      changeQueue,
      errorHub,
      state,
      startupLatch,
      shutdownLatch,
      shutdownSignal
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
    def updateWebhookState(id: WebhookId, newWebhookState: WebhookState): InternalState =
      copy(webhookState = webhookState.updated(id, newWebhookState))

    def suspendRetries(timestamp: Instant): InternalState =
      copy(webhookState = webhookState.map {
        case (id, retrying: WebhookState.Retrying) =>
          (id, retrying.suspend(timestamp))
        case (id, state)                           => (id, state)
      })
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

  def shutdown: ZIO[Has[WebhookServer] with Clock, IOException, Any] =
    ZIO.environment[Has[WebhookServer] with Clock].flatMap(_.get[WebhookServer].shutdown)

  /**
   * [[WebhookState]] is the server's internal representation of a webhook's state.
   */
  private[webhooks] sealed trait WebhookState extends Product with Serializable
  private[webhooks] object WebhookState {
    sealed trait IncomingChange extends Product with Serializable
    object IncomingChange {
      final case class ToRetrying(id: WebhookId, retrying: Retrying) extends IncomingChange
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
      retryQueue: Queue[WebhookEvent],
      lastRetryTime: Instant,
      base: Duration,
      power: Double,
      timeLeft: Duration,
      nextBackoff: Duration,
      attempt: Int = 0,
      inFlight: Map[WebhookEventKey, WebhookEvent] = Map.empty
    ) extends WebhookState {
      def addInFlight(events: Iterable[WebhookEvent]): Retrying    =
        copy(inFlight = inFlight ++ events.map(ev => ev.key -> ev))

      def enqueueAll(events: Iterable[WebhookEvent]): UIO[Boolean] =
        retryQueue.offerAll(events)

      def removeInFlight(events: Iterable[WebhookEvent]): Retrying =
        copy(inFlight = inFlight.removeAll(events.map(_.key)))

      def requeue(events: NonEmptyChunk[WebhookEvent]): URIO[Clock, Boolean] =
        retryQueue.offerAll(events).delay(nextBackoff)

      /**
       * Progresses retrying to the next exponential backoff.
       */
      def increaseBackoff(timestamp: Instant): Retrying =
        copy(
          lastRetryTime = timestamp,
          nextBackoff = base * math.pow(2, attempt.toDouble),
          attempt = attempt + 1
        )

      /**
       * Resets backoff
       */
      def resetBackoff(timestamp: Instant): Retrying =
        copy(lastRetryTime = timestamp, nextBackoff = base, attempt = 0)

      /**
       * Suspends this retry by replacing the backoff with the time left until its backoff completes.
       */
      def suspend(now: Instant): Retrying =
        copy(
          nextBackoff = nextBackoff.minus(JDuration.between(now, lastRetryTime)),
          timeLeft = timeLeft.minus(Duration.fromInterval(sinceTime, now))
        )
    }

    case object Unavailable extends WebhookState
  }
}
