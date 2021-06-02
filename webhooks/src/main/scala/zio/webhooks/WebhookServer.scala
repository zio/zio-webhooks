package zio.webhooks

import zio._
import zio.clock._
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.stream.ZStream
import zio.webhooks.WebhookDeliveryBatching._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServer._

import java.io.IOException
import java.time.Duration
import java.time.Instant

/**
 * A [[WebhookServer]] subscribes to webhook events and reliably dispatches them, i.e. failed
 * dispatches are attempted twice followed by exponential backoff. Retries are performed until some
 * duration after which webhooks will be marked `Unavailable` since some [[java.time.Instant]].
 *
 * Batched deliveries are enabled if `batchConfig` is defined.
 *
 * A live server layer is provided in the companion object for convenience and proper resource
 * management.
 */
final case class WebhookServer(
  webhookRepo: WebhookRepo,
  stateRepo: WebhookStateRepo,
  eventRepo: WebhookEventRepo,
  httpClient: WebhookHttpClient,
  batchingQueue: Queue[(Webhook, WebhookEvent)],
  errorHub: Hub[WebhookError],
  webhookState: Ref[Map[WebhookId, WebhookServer.WebhookState]],
  batchingConfig: Option[BatchingConfig]
) {

  private def consumeBatchElements(maxBatchSize: Int, maxWaitTime: Duration): URIO[Clock, Unit] =
    UStream
      .fromQueue(batchingQueue)
      // TODO: make key (webhook ID, content type) combo
      .groupByKey(_._1, maxBatchSize) { (webhook, stream) =>
        stream
          .map(_._2)
          .groupedWithin(maxBatchSize, maxWaitTime)
          .map(NonEmptyChunk.fromChunk(_))
          .collectSome
          .mapM(events => dispatch(WebhookDispatch(webhook, events)))
      }
      .runDrain

  private def dispatch(dispatch: WebhookDispatch): UIO[Unit] = {
    // TODO: Events may have different headers,
    // TODO: we're just taking the last one's for now.
    // TODO: WebhookEvents' contents are just being appended.
    // TODO: Content is stringly-typed, may need more structure
    val request = WebhookHttpRequest(
      dispatch.webhook.url,
      dispatch.events.map(_.content).mkString("\n"),
      dispatch.events.last.headers
    )
    for {
      _ <- httpClient.post(request).ignore // TODO: handle errors/non-200
      // TODO[design]: should have setEventStatus variant accepting multiple keys
      _ <- ZIO.foreach(dispatch.events) { event =>
             eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivered).ignore
           }
      // TODO: enqueue webhook/event errors
    } yield ()
  }

  private def dispatchNewEvent(webhook: Webhook, event: WebhookEvent): IO[WebhookError, Unit] =
    for {
      _ <- eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivering)
      _ <- webhook.batching match {
             case Single  =>
               dispatch(WebhookDispatch(webhook, NonEmptyChunk(event)))
             case Batched =>
               val deliveringEvent = event.copy(status = WebhookEventStatus.Delivering)
               batchingQueue.offer((webhook, deliveringEvent))
           }
    } yield ()

  def getErrors: UStream[WebhookError] =
    UStream.fromHub(errorHub)

  /**
   * Starts the webhook server. Kicks off the following to run concurrently:
   * - new webhook event subscription
   * - event recovery for webhooks which need to deliver at least once
   * - dispatch retry monitoring
   * - batching
   */
  // TODO: should retry monitoring just be dispatch?
  def start: URIO[Clock, Any] =
    startNewEventSubscription *> startEventRecovery *> startRetryMonitoring *> startBatching

  /**
   * Starts a fiber that listens to events queued for batched webhook dispatch
   */
  private def startBatching: URIO[Clock, Any] =
    batchingConfig match {
      case None                                       =>
        ZIO.unit
      case Some(BatchingConfig(maxSize, maxWaitTime)) =>
        consumeBatchElements(maxSize, maxWaitTime).forkAs("batching")
    }

  // get events that are Delivering whose webhooks have AtLeastOnce delivery semantics.
  // reconstruct webhookState
  /**
   * Starts recovery of events whose status is `Delivering` for webhooks with `AtLeastOnce`
   * delivery semantics.
   */
  private def startEventRecovery: UIO[Any] = ZIO.unit.fork

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
  private def startNewEventSubscription: UIO[Any] = {
    for (newEvent <- eventRepo.getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))) {
      val webhookId = newEvent.key.webhookId
      webhookRepo
        .getWebhookById(webhookId)
        .flatMap(ZIO.fromOption(_).mapError(_ => MissingWebhookError(webhookId)))
        .flatMap(webhook => ZIO.when(webhook.isOnline)(dispatchNewEvent(webhook, newEvent)))
        .catchAll(errorHub.publish(_).unit)
    }
  }.forkAs("new-event-subscription")

  // retry dispatching every WebhookEvent in queues twice, then exponentially
  // if we've retried for >7 days,
  //   mark Webhook as Unavailable
  //   clear WebhookState for that Webhook
  //   make sure that startNewEventSubscription does _not_ try to deliver to an unavailable webhook
  /**
   * Kicks off backoff retries for every [[WebhookEvent]] pending delivery.
   */
  private def startRetryMonitoring: UIO[Any] = ZIO.unit.fork

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit
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

  def getErrors: ZStream[Has[WebhookServer], Nothing, WebhookError] =
    ZStream.service[WebhookServer].flatMap(_.getErrors)

  val live: URLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      state          <- Ref.makeManaged(Map.empty[WebhookId, WebhookServer.WebhookState])
      batchQueue     <- Queue.bounded[(Webhook, WebhookEvent)](1024).toManaged_
      errorHub       <- Hub.sliding[WebhookError](128).toManaged_
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
                          batchingConfig
                        )
      _              <- server.start.toManaged_
      _              <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server
  }.toLayer

  sealed trait WebhookState
  object WebhookState {
    case object Enabled                                                       extends WebhookState
    case object Disabled                                                      extends WebhookState
    // TODO[high-prio]: replace WebhookEvent with a Dispatch type, as retries should be done on dispatches
    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookEvent]) extends WebhookState
    case object Unavailable                                                   extends WebhookState
  }
}
