package zio.webhooks

import zio._
import zio.clock.Clock
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhookDeliveryBatching._
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer._

import java.io.IOException
import java.time.Instant

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
final class WebhookServer private (
  private val webhookRepo: WebhookRepo,
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val config: WebhookServerConfig,
  private val errorHub: Hub[WebhookError],
  private val internalState: RefM[InternalState],
  private val batchingQueue: Option[Queue[(Webhook, WebhookEvent)]],
  private val changeQueue: Queue[WebhookState.Change]
) {

  /**
   * Attempts delivery of a [[WebhookDispatch]] to the webhook receiver. On successful delivery,
   * dispatched events are marked [[WebhookEventStatus.Delivered]]. On failure, retries are
   * enqueued for events from webhooks with at-least-once delivery semantics.
   */
  private def deliver(dispatch: WebhookDispatch): URIO[Clock, Unit] = {
    def startRetrying(id: WebhookId, state: InternalState) =
      for {
        instant       <- clock.instant
        _             <- webhookRepo.setWebhookStatus(id, WebhookStatus.Retrying(instant))
        retryingState <- WebhookState.Retrying.make(config.retry.capacity)
        _             <- retryingState.queue.offer(dispatch)
        _             <- changeQueue.offer(WebhookState.Change.ToRetrying(id, retryingState.queue))
      } yield state.updateWebhookState(id, retryingState)

    def handleAtLeastOnce = {
      val id = dispatch.webhook.id
      internalState.update { internalState =>
        internalState.webhookState.get(id) match {
          case Some(WebhookState.Enabled)            =>
            startRetrying(id, internalState)
          case None                                  =>
            startRetrying(id, internalState)
          case Some(WebhookState.Retrying(_, queue)) =>
            queue.offer(dispatch) *> UIO(internalState)
          case Some(WebhookState.Disabled)           =>
            ??? // TODO: handle
          case Some(WebhookState.Unavailable)        =>
            ??? // TODO: handle
        }
      }
    }

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
               deliver(WebhookDispatch(webhook, NonEmptyChunk(event)))
           }
    } yield ()

  /**
   * Exposes a way to listen for [[WebhookError]]s, namely missing webhooks or events. This provides
   * clients a way to handle server errors that would otherwise just fail silently.
   */
  def getErrors: UManaged[Dequeue[WebhookError]] =
    errorHub.subscribe

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
      _     <- startEventRecovery
      _     <- startRetryMonitoring
      latch <- Promise.make[Nothing, Unit]
      _     <- startNewEventSubscription(latch)
      _     <- startBatching
      _     <- latch.await
    } yield ()

  /**
   * Starts a fiber that listens to events queued for batched webhook dispatch.
   */
  private def startBatching: URIO[Clock, Fiber.Runtime[Nothing, Unit]] =
    (config.batching, batchingQueue) match {
      case (Some(WebhookServerConfig.Batching(_, maxSize, maxWaitTime)), Some(batchingQueue)) => {
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
      case _                                                                                  =>
        ZIO.unit.fork
    }

  /**
   * Starts recovery of events with status [[WebhookEventStatus.Delivering]] for webhooks with
   * [[WebhookDeliverySemantics.AtLeastOnce]]. Recovery is done by reconstructing
   * [[WebhookServer.WebhookState]], the server's internal representation of webhooks it handles.
   * This ensures retries continue after a server is restarted.
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
            .flatMap(webhook => dispatchNewEvent(webhook, newEvent).when(webhook.isAvailable))
            .catchAll(errorHub.publish(_).unit)
        }
      }
      .forkAs("new-event-subscription")

  /**
   * Starts retries a webhook's queue of dispatches. Retrying is done until the queue is exhausted.
   * If retrying times out, the webhook is set to [[WebhookStatus.Unavailable]] and all its events
   * are marked [[WebhookEventStatus.Failed]].
   */
  private def startRetrying(id: WebhookId, queue: Queue[WebhookDispatch]) =
    for {
      success   <- takeAndRetry(queue)
                     .repeat(
                       Schedule.recurUntil[Int](_ == 0) &&
                         Schedule.exponential(config.retry.exponentialBase, config.retry.exponentialFactor)
                     )
                     .timeoutTo(None)(Some(_))(config.retry.timeout)
      newStatus <- if (success.isDefined)
                     ZIO.succeed(WebhookStatus.Enabled)
                   else
                     clock.instant.map(WebhookStatus.Unavailable) <& eventRepo.setAllAsFailedByWebhookId(id)
      _         <- webhookRepo.setWebhookStatus(id, newStatus)
      _         <- internalState.update(state => UIO(state.updateWebhookState(id, WebhookState.from(newStatus))))
    } yield ()

  /**
   * Starts monitoring for internal webhook state changes, i.e. [[WebhookState.Change.ToRetrying]].
   */
  private def startRetryMonitoring = {
    for {
      update <- changeQueue.take
      _      <- update match {
                  case WebhookState.Change.ToRetrying(id, queue) =>
                    startRetrying(id, queue).catchAll(errorHub.publish).fork
                }
    } yield ()
  }.forever.forkAs("retry-monitoring")

  // let batching finish
  // let in-flight retry requests finish (maybe fork them uninterruptibly)
  // persist retry state for each webhook
  // check for shutdown before handling new events
  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit

  /**
   * Takes a dispatch from a retry queue and attempts delivery. When successful, dispatch events are
   * marked [[WebhookEventStatus.Delivered]]. If the dispatch fails, is put back into the queue to
   * be retried again.
   *
   * Returns the current queue size.
   */
  private def takeAndRetry(retryQueue: Queue[WebhookDispatch]) =
    for {
      dispatch    <- retryQueue.take
      response    <- httpClient.post(WebhookHttpRequest.fromDispatch(dispatch)).option
      _           <- response match {
                       case Some(WebhookHttpResponse(200)) =>
                         if (dispatch.size == 1)
                           eventRepo.setEventStatus(dispatch.head.key, WebhookEventStatus.Delivered)
                         else
                           eventRepo.setEventStatusMany(dispatch.keys, WebhookEventStatus.Delivered)
                       case _                              =>
                         retryQueue.offer(dispatch)
                     }
      currentSize <- retryQueue.size
    } yield currentSize
}

object WebhookServer {

  /**
   * Creates a server, pulling dependencies from the environment while initializing internal state.
   */
  def create: URIO[Env, WebhookServer] =
    for {
      serverConfig  <- ZIO.service[WebhookServerConfig]
      webhookRepo   <- ZIO.service[WebhookRepo]
      eventRepo     <- ZIO.service[WebhookEventRepo]
      httpClient    <- ZIO.service[WebhookHttpClient]
      state         <- RefM.make(InternalState(isShutdown = false, Map.empty))
      errorHub      <- Hub.sliding[WebhookError](serverConfig.errorSlidingCapacity)
      batchingQueue <- ZIO
                         .foreach(serverConfig.batching) { batching =>
                           Queue.bounded[(Webhook, WebhookEvent)](batching.capacity)
                         }
      changeQueue   <- Queue.bounded[WebhookState.Change](1)
    } yield new WebhookServer(
      webhookRepo,
      eventRepo,
      httpClient,
      serverConfig,
      errorHub,
      state,
      batchingQueue,
      changeQueue
    )

  type Env = Has[WebhookRepo]
    with Has[WebhookStateRepo]
    with Has[WebhookEventRepo]
    with Has[WebhookHttpClient]
    with Has[WebhookServerConfig]
    with Clock

  def getErrors: URManaged[Has[WebhookServer], Dequeue[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.getErrors)

  private final case class InternalState(isShutdown: Boolean, webhookState: Map[WebhookId, WebhookState]) {
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

  def shutdown: ZIO[Has[WebhookServer], IOException, Any] =
    ZIO.serviceWith(_.shutdown)

  private sealed trait WebhookState extends Product with Serializable
  private object WebhookState {
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

    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookDispatch]) extends WebhookState

    object Retrying {
      def make(capacity: Int): URIO[Clock, Retrying] =
        ZIO.mapN(clock.instant, Queue.bounded[WebhookDispatch](capacity))(Retrying(_, _))
    }

    case object Unavailable extends WebhookState
  }
}
