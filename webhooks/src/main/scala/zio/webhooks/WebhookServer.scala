package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks.WebhookDeliveryBatching._
import zio.webhooks.WebhookError._

import java.io.IOException
import java.time.Instant

/**
 * A [[WebhookServer]] subscribes to webhook events and reliably dispatches them, i.e. failed
 * dispatches are retried until some set time.
 *
 * A live server layer is provided for convenience and to ensure proper resource management.
 */
final case class WebhookServer(
  webhookRepo: WebhookRepo,
  stateRepo: WebhookStateRepo,
  eventRepo: WebhookEventRepo,
  httpClient: WebhookHttpClient,
  webhookState: SubscriptionRef[Map[WebhookId, WebhookServer.WebhookState]],
  batchConfig: Option[WebhookServer.BatchingConfig] = None
) {

  private def dispatchEvent(webhook: Webhook, event: WebhookEvent) =
    for {
      _      <- eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivering)
      request = WebhookHttpRequest(webhook.url, event.content, event.headers)
      _      <- webhook.batching match {
                  case Single     =>
                    dispatchRequest(request)
                  case Batched(_) => ???
                }
      _      <- eventRepo.setEventStatus(event.key, WebhookEventStatus.Delivered)
    } yield ()

  private def dispatchRequest(request: WebhookHttpRequest): URIO[Any, Unit] =
    // TODO: handle post non-200, IOException
    httpClient.post(request).ignore

  /**
   * Starts the webhook server. Kicks off the following to run concurrently:
   * - new webhook event subscription
   * - event recovery for webhooks which need to deliver at least once
   * - retry monitoring
   */
  def start: IO[WebhookError, Any] =
    startNewEventSubscription *> startEventRecovery *> startRetryMonitoring *> startBatching
  // TODO[high-prio]: handle the first error from any of the fibers, smth like:
  // startNewEventSubscription raceFirst startEventRecovery raceFirst startRetryMonitoring

  /**
   * Kicks off a fiber that listens to events queued for webhook batch dispatch
   *
   * @return
   */
  private def startBatching: UIO[Fiber[WebhookError, Unit]] = {
    batchConfig match {
      case None             => ZIO.unit
      case Some(config @ _) => ZIO.unit
    }
  }.fork

  // get events that are Delivering & AtLeastOnce
  // reconstruct webhookState
  /**
   * Kicks off recovery of events whose status is `Delivering` for webhooks with `AtLeastOnce`
   * delivery semantics.
   */
  private def startEventRecovery: UIO[Fiber[WebhookError, Unit]] = ZIO.unit.fork

  /**
   * Call webhookEventRepo.getEventsByStatus looking for new events
   *
   * WebhookEventStatus.New
   *
   * For each new event:
   *  - mark event as Delivering
   *  - check to see if webhookId is retrying
   *    - if there are queues, we're retrying
   *      - enqueue the event
   *    - otherwise:
   *      - send it
   *        - if successful
   *          - mark it as Delivered
   *        - if unsuccessful
   *          - create a queue in the state
   *          - enqueue the event into the queue
   */
  /**
   * Kicks off new [[WebhookEvent]] subscription.
   */
  private def startNewEventSubscription: UIO[Fiber[WebhookError, Unit]] =
    eventRepo
      .subscribeToEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .foreach { newEvent =>
        val webhookId = newEvent.key.webhookId
        for {
          webhook <- webhookRepo
                       .getWebhookById(webhookId)
                       .flatMap(ZIO.fromOption(_).mapError(_ => MissingWebhookError(webhookId)))
          _       <- ZIO.when(webhook.isOnline)(dispatchEvent(webhook, newEvent))
        } yield ()
      }
      .forkAs("new event subscription")

  // retry dispatching every WebhookEvent in queues twice, then exponentially
  // if we've retried for >7 days,
  //   mark Webhook as Unavailable
  //   clear WebhookState for that Webhook
  //   make sure that startNewEventSubscription does _not_ try to deliver to an unavailable webhook
  /**
   * Kicks off backoff retries for every [[WebhookEvent]] pending delivery.
   */
  private def startRetryMonitoring: UIO[Fiber[WebhookError, Unit]] = ZIO.unit.fork

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit
}

object WebhookServer {

  case class Batch(maxSize: Int, webhook: Webhook, events: Chunk[WebhookEvent])

  // TODO: add/implement maxWait duration
  final case class BatchingConfig(maxSize: Int)

  type Env = Has[WebhookRepo] with Has[WebhookStateRepo] with Has[WebhookEventRepo] with Has[WebhookHttpClient]

  val live: RLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      state      <- SubscriptionRef.make(Map.empty[WebhookId, WebhookServer.WebhookState]).toManaged_
      repo       <- ZManaged.service[WebhookRepo]
      stateRepo  <- ZManaged.service[WebhookStateRepo]
      eventRepo  <- ZManaged.service[WebhookEventRepo]
      httpClient <- ZManaged.service[WebhookHttpClient]
      server      = WebhookServer(repo, stateRepo, eventRepo, httpClient, state)
      _          <- server.start.mapError(_.asThrowable).orDie.toManaged_
      _          <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server
  }.toLayer

  sealed trait WebhookState
  object WebhookState {
    final case class Enabled(batch: Option[Batch])                            extends WebhookState
    case object Disabled                                                      extends WebhookState
    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookEvent]) extends WebhookState
    case object Unavailable                                                   extends WebhookState
  }
}
