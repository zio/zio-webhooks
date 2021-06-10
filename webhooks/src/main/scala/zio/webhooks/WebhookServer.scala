package zio.webhooks

import zio._
import zio.clock.{ instant, Clock }
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhookDeliveryBatching._
import zio.webhooks.WebhookDeliverySemantics._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer._

import java.io.IOException
import java.time.{ Duration, Instant }

/**
 * A [[WebhookServer]] subscribes to webhook events and reliably dispatches them, i.e. failed
 * dispatches are attempted twice followed by exponential backoff. Retries are performed until some
 * duration after which webhooks will be marked `Unavailable` since some [[java.time.Instant]].
 *
 * Dispatches are batched if and only if a `batchConfig` is defined ''and'' a webhook's delivery mode
 * is set to `Batched`.
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

  private def consumeBatchElements(maxBatchSize: Int, maxWaitTime: Duration): URIO[Clock, Unit] =
    UStream
      .fromQueue(batchingQueue)
      .groupByKey(
        pair => {
          val (webhook, event) = pair
          (webhook.id, event.headers.find(_._1.toLowerCase == "content-type"))
        },
        maxBatchSize
      ) {
        case (_, stream) =>
          stream
            .groupedWithin(maxBatchSize, maxWaitTime)
            .map(NonEmptyChunk.fromChunk)
            .collectSome
            .mapM(events => deliver(WebhookDispatch(events.head._1, events.map(_._2))))
      }
      .runDrain

  private def deliver(dispatch: WebhookDispatch): URIO[Clock, Unit] = {
    def startRetrying(id: WebhookId, map: Map[WebhookId, WebhookState]) =
      for {
        instant       <- clock.instant
        _             <- webhookRepo.setWebhookStatus(id, WebhookStatus.Retrying(instant))
        retryingState <- WebhookState.Retrying.make(128)
        stateChange    = WebhookState.Change(id, WebhookState.Enabled, retryingState)
        _             <- retryingState.queue.offer(dispatch)
        _             <- changeQueue.offer(stateChange)
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
                  ???
                case Some(WebhookState.Unavailable)        =>
                  ???
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
               val deliveringEvent = event.copy(status = WebhookEventStatus.Delivering)
               batchingQueue.offer((webhook, deliveringEvent))
             case _                  =>
               deliver(WebhookDispatch(webhook, NonEmptyChunk(event)))
           }
    } yield ()

  def getErrors: UManaged[UStream[WebhookError]] =
    UStream.fromHubManaged(errorHub)

  /**
   * Starts the webhook server. Kicks off the following to run concurrently:
   *
   *   - new webhook event subscription
   *   - event recovery for webhooks which need to deliver at least once
   *   - dispatch retry monitoring
   *   - batching
   */
  def start: URIO[Clock, Any] =
    for {
      latch <- Promise.make[Nothing, Unit]
      _     <- startNewEventSubscription(latch)
      _     <- startEventRecovery
      _     <- startRetryMonitoring // TODO[design]: should retry monitoring just be dispatch?
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
      case Some(BatchingConfig(maxSize, maxWaitTime)) =>
        consumeBatchElements(maxSize, maxWaitTime).forkAs("batching")
    }

  // get events that are Delivering whose webhooks have AtLeastOnce delivery semantics.
  // reconstruct webhookState

  /**
   * Starts recovery of events whose status is `Delivering` for webhooks with `AtLeastOnce`
   * delivery semantics.
   */
  private def startEventRecovery: UIO[Unit] = ZIO.unit

  // Call webhookEventRepo.getEventsByStatus looking for new events
  //
  // For each new event:
  //  - mark event as Delivering
  //  - check to see if webhookId is retrying
  //    - if there are queues, we're retrying
  //      - enqueue the event
  //    - otherwise:
  //      - send it
  //        - if successful
  //          - mark it as Delivered
  //        - if unsuccessful
  //          - create a queue in the state
  //          - enqueue the event into the queue
  //

  /**
   * Kicks off new [[WebhookEvent]] subscription.
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

  // retry dispatching every WebhookDispatch in queues twice, then exponentially
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
                  case WebhookState.Change(id, WebhookState.Enabled, WebhookState.Retrying(sinceTime @ _, queue)) => {
                      for {
                        _ <- takeAndRetry(queue).repeatUntil(_ == 0)
                        _ <- queue.shutdown
                        _ <- webhookRepo.setWebhookStatus(id, WebhookStatus.Enabled)
                        _ <- webhookState.get.map(_.updated(id, WebhookState.Enabled))
                      } yield ()
                    }.fork
                  case _                                                                                          =>
                    ???
                }
    } yield ()
  }.forever.forkAs("retry-monitoring")

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit

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
    val default: ULayer[Has[Option[BatchingConfig]]] = live(10, Duration.ofSeconds(5))

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

  def getErrors: URManaged[Has[WebhookServer], UStream[WebhookError]] =
    ZManaged.service[WebhookServer].flatMap(_.getErrors)

  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      state          <- RefM.makeManaged(Map.empty[WebhookId, WebhookServer.WebhookState])
      batchQueue     <- Queue.bounded[(Webhook, WebhookEvent)](1024).toManaged_
      errorHub       <- Hub.sliding[WebhookError](128).toManaged_
      changeQueue    <- Queue.bounded[WebhookState.Change](1).toManaged_
      batchingConfig <- ZManaged.service[Option[BatchingConfig]]
      repo           <- ZManaged.service[WebhookRepo]
      stateRepo      <- ZManaged.service[WebhookStateRepo]
      eventRepo      <- ZManaged.service[WebhookEventRepo]
      httpClient     <- ZManaged.service[WebhookHttpClient]
      server          = WebhookServer(
                          repo,
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

  sealed trait WebhookState

  object WebhookState {
    final case class Change(id: WebhookId, from: WebhookState, to: WebhookState)

    case object Disabled extends WebhookState

    case object Enabled extends WebhookState

    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookDispatch]) extends WebhookState

    object Retrying {
      def make(capacity: Int): URIO[Clock, Retrying] =
        ZIO.mapN(instant, Queue.bounded[WebhookDispatch](capacity))(Retrying(_, _))
    }

    case object Unavailable extends WebhookState
  }
}
