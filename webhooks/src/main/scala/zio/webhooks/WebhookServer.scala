package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhookDeliveryBatching._
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer._

import java.io.IOException
import java.time.{ Duration, Instant }

/**
 * A [[WebhookServer]] subscribes to [[WebhookEvent]]s and reliably delivers them, i.e. failed
 * dispatches are retried once, followed by retries with exponential backoff. Retries are performed
 * until some duration after which webhooks will be marked [[WebhookStatus.Unavailable]] since some
 * [[java.time.Instant]]. Dispatches are batched iff a `batchConfig` is defined ''and'' a webhook's
 * delivery batching is [[WebhookDeliveryBatching.Batched]].
 *
 * A live server layer is provided in the companion object for convenience and proper resource
 * management.
 */
final case class WebhookServer( // TODO: split server into components, this is looking a little too much 😬
  webhookRepo: WebhookRepo,
  stateRepo: WebhookStateRepo,
  eventRepo: WebhookEventRepo,
  httpClient: WebhookHttpClient,
  batchingQueue: Queue[(Webhook, WebhookEvent)],
  errorHub: Hub[WebhookError],
  webhookState: RefM[Map[WebhookId, WebhookServer.WebhookState]],
  changeQueue: Queue[WebhookState.Change],
  batchingConfig: Option[BatchingConfig] = None
) {

  private def deliver(dispatch: WebhookDispatch): URIO[Clock, Unit] = {
    def startRetrying(id: WebhookId, map: Map[WebhookId, WebhookState]) =
      for {
        instant       <- clock.instant
        _             <- webhookRepo.setWebhookStatus(id, WebhookStatus.Retrying(instant))
        retryingState <- WebhookState.Retrying.make(128) // TODO: add retry capacity to config
        _             <- retryingState.queue.offer(dispatch)
        _             <- changeQueue.offer(WebhookState.Change.ToRetrying(id, retryingState.queue))
      } yield map.updated(id, retryingState)

    for {
      response <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      _        <- {
        (dispatch.semantics, response) match {
          case (_, Some(WebhookHttpResponse(200))) =>
            if (dispatch.size == 1)
              eventRepo.setEventStatus(dispatch.head.key, WebhookEventStatus.Delivered)
            else
              eventRepo.setEventStatusMany(dispatch.keys, WebhookEventStatus.Delivered)
          case (AtLeastOnce, _)                    =>
            val id = dispatch.webhook.id
            webhookState.update { map =>
              map.get(id) match {
                case Some(WebhookState.Enabled)            =>
                  startRetrying(id, map)
                case None                                  =>
                  startRetrying(id, map)
                case Some(WebhookState.Retrying(_, queue)) =>
                  queue.offer(dispatch) *> UIO(map)
                case Some(WebhookState.Disabled)           =>
                  ??? // TODO: handle
                case Some(WebhookState.Unavailable)        =>
                  ??? // TODO: handle
              }
            }
          case (AtMostOnce, _)                     =>
            eventRepo.setEventStatusMany(dispatch.events.map(_.key), WebhookEventStatus.Failed)
        }
      }.catchAll(errorHub.publish)
    } yield ()
  }

  private def dispatchNewEvent(webhook: Webhook, event: WebhookEvent): ZIO[Clock, WebhookError, Unit] =
    for {
      _ <- eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivering)
      _ <- (webhook.batching, batchingConfig) match {
             case (Batched, Some(_)) =>
               batchingQueue.offer((webhook, event.copy(status = WebhookEventStatus.Delivering)))
             case _                  =>
               deliver(WebhookDispatch(webhook, NonEmptyChunk(event)))
           }
    } yield ()

  def getErrors: UManaged[Dequeue[WebhookError]] =
    errorHub.subscribe

  /**
   * Starts the webhook server. Kicks off the following to run concurrently:
   *
   *   - new webhook event subscription
   *   - event recovery for webhooks which need to deliver at least once
   *   - dispatch retry monitoring
   *   - dispatch batching, if configured and enabled per webhook
   */
  def start: URIO[Clock, Any] =
    for {
      latch <- Promise.make[Nothing, Unit]
      _     <- startNewEventSubscription(latch)
      _     <- startEventRecovery
      _     <- startRetryMonitoring
      _     <- startBatching
      // wait for subscriptions to be ready
      _     <- latch.await
    } yield ()

  /**
   * Starts a fiber that listens to events queued for batched webhook dispatch
   */
  private def startBatching: URIO[Clock, Fiber.Runtime[Nothing, Unit]] =
    batchingConfig match {
      case None                                       =>
        ZIO.unit.fork
      case Some(BatchingConfig(maxSize, maxWaitTime)) => {
          val getWebhookIdAndContentType = (webhook: Webhook, event: WebhookEvent) =>
            (webhook.id, event.headers.find(_._1.toLowerCase == "content-type"))

          UStream
            .fromQueue(batchingQueue)
            .groupByKey(getWebhookIdAndContentType.tupled, maxSize) {
              case (_, stream) =>
                stream
                  .groupedWithin(maxSize, maxWaitTime)
                  .map(NonEmptyChunk.fromChunk)
                  .collectSome
                  .mapM(events => deliver(WebhookDispatch(events.head._1, events.map(_._2))))
            }
            .runDrain
        }.forkAs("batching")
    }

  // get events that are Delivering whose webhooks have AtLeastOnce delivery semantics.
  // reconstruct webhookState

  /**
   * Starts recovery of events whose status is `Delivering` for webhooks with `AtLeastOnce`
   * delivery semantics. Recovery is done by reconstructing [[WebhookServer.WebhookState]], the
   * server's internal representation of webhooks it handles. This is especially important.
   */
  private def startEventRecovery: UIO[Unit] = ZIO.unit

  /**
   * Starts new [[WebhookEvent]] subscription. Takes a latch which succeeds when the server is ready
   * to receive events.
   */
  private def startNewEventSubscription(latch: Promise[Nothing, Unit]) =
    eventRepo
      .getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .use { dequeue =>
        dequeue.poll *> latch.succeed(()) *> UStream.fromQueue(dequeue).foreach { newEvent =>
          val webhookId = newEvent.key.webhookId
          webhookRepo
            .getWebhookById(webhookId)
            .flatMap(ZIO.fromOption(_).orElseFail(MissingWebhookError(webhookId)))
            .flatMap(webhook => dispatchNewEvent(webhook, newEvent).when(webhook.isOnline))
            .catchAll(errorHub.publish(_).unit)
        }
      }
      .forkAs("new-event-subscription")

  private def startRetrying(id: WebhookId, queue: Queue[WebhookDispatch]) =
    for {
      // TODO: replace 7-day timeout, 10.millis base with config
      success   <- takeAndRetry(queue)
                     .repeat(Schedule.recurUntil[Int](_ == 0) && Schedule.exponential(10.millis))
                     .timeoutTo(None)(Some(_))(7.days)
      newStatus <- if (success.isDefined) ZIO.succeed(WebhookStatus.Enabled)
                   else clock.instant.map(WebhookStatus.Unavailable) <& eventRepo.setAllAsFailedByWebhookId(id)
      _         <- webhookRepo.setWebhookStatus(id, newStatus)
      _         <- webhookState.update(map => UIO(map.updated(id, WebhookState.Enabled)))
    } yield ()

  // TODO: add some of below to docs
  // try dispatching every WebhookDispatch in queues twice, then exponentially
  // if we've retried for >7 days,
  //   mark Webhook as Unavailable
  //   clear WebhookState for that Webhook
  //   make sure that startNewEventSubscription does _not_ try to deliver to an unavailable webhook
  /**
   * Kicks off backoff retries for every [[WebhookEvent]] pending delivery.
   */
  private def startRetryMonitoring = {
    for {
      update <- changeQueue.take
      _      <- update match {
                  case WebhookState.Change.ToRetrying(id, queue) =>
                    startRetrying(id, queue).fork
                }
    } yield ()
  }.forever.forkAs("retry-monitoring")

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit

  // TODO: Scaladoc
  private def takeAndRetry(queue: Queue[WebhookDispatch]) =
    for {
      dispatch    <- queue.take
      response    <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      _           <- response match {
                       case Some(WebhookHttpResponse(200)) =>
                         if (dispatch.size == 1)
                           eventRepo.setEventStatus(dispatch.head.key, WebhookEventStatus.Delivered)
                         else
                           eventRepo.setEventStatusMany(dispatch.keys, WebhookEventStatus.Delivered)
                       case _                              =>
                         queue.offer(dispatch)
                     }
      currentSize <- queue.size
    } yield currentSize
}

object WebhookServer {
  // TODO: Smart constructor
  final case class BatchingConfig(maxSize: Int, maxWaitTime: Duration)

  object BatchingConfig {
    val default: ULayer[Has[Option[BatchingConfig]]] = live(10, 5.seconds)

    val disabled: ULayer[Has[Option[BatchingConfig]]] =
      ZLayer.succeed(None)

    def live(maxSize: Int, maxWaitTime: Duration): ULayer[Has[Option[BatchingConfig]]] =
      ZLayer.succeed(Some(BatchingConfig(maxSize, maxWaitTime)))
  }

  type Env = Has[WebhookRepo]
    with Has[WebhookStateRepo]
    with Has[WebhookEventRepo]
    with Has[WebhookHttpClient]
    with Has[Option[BatchingConfig]]
    with Clock

  def getErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.getErrors)

  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      state          <- RefM.makeManaged(Map.empty[WebhookId, WebhookServer.WebhookState])
      batchQueue     <- Queue.bounded[(Webhook, WebhookEvent)](1024).toManaged_
      errorHub       <- Hub.sliding[WebhookError](128).toManaged_
      changeQueue    <- Queue.bounded[WebhookState.Change](1).toManaged_
      batchingConfig <- ZManaged.service[Option[BatchingConfig]]
      webhookRepo    <- ZManaged.service[WebhookRepo]
      stateRepo      <- ZManaged.service[WebhookStateRepo]
      eventRepo      <- ZManaged.service[WebhookEventRepo]
      httpClient     <- ZManaged.service[WebhookHttpClient]
      server          = WebhookServer(
                          webhookRepo,
                          stateRepo,
                          eventRepo,
                          httpClient,
                          batchQueue,
                          errorHub,
                          state,
                          changeQueue,
                          batchingConfig
                        )
      _              <- server.start.toManaged_
      _              <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server
  }.toLayer

  sealed trait WebhookState extends Product with Serializable
  object WebhookState {
    sealed trait Change
    object Change {
      final case class ToRetrying(id: WebhookId, queue: Queue[WebhookDispatch]) extends Change
    }

    case object Disabled extends WebhookState

    case object Enabled extends WebhookState

    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookDispatch]) extends WebhookState

    object Retrying {
      def make(capacity: Int): URIO[Clock, Retrying] =
        ZIO.mapN(clock.instant, Queue.bounded[WebhookDispatch](capacity))(Retrying(_, _))
    }

    case object Unavailable extends WebhookState
  }
}
