package zio.webhooks

import zio._
import java.io.IOException
import java.time.Instant

final case class WebhookServer(
  webhookRepo: WebhookRepo,
  webhookStateRepo: WebhookStateRepo,
  webhookEventRepo: WebhookEventRepo,
  // consider STM if needed
  webhookState: Ref[Map[WebhookId, WebhookServer.WebhookState]]
) {

  /**
   * Subscribe
   */
  def start: IO[IOException, Any] =
    // start fibers for each
    subscribeToNewEvents *> initiateRecovery *> monitorRetries

  // periodically retry every WebhookEvent in queue
  // if we've retried for >7 days,
  //   Webhook as Unavailable
  //   clear Queue for that Webhook
  //   make sure that subscribeToNewEvents does _not_ try to deliver to an unavailable webhook
  def monitorRetries: UIO[Any] = ZIO.unit // ticket

  /**
   * Call webhookEventRepo.getEventsByStatus looking for new events
   *
   * WebhookEventStatus.New
   *
   * For each new event:
   *  - mark event as Delivering
   *  - check to see if webhookId is retrying
   *    - if there's a queue in the map, we're retrying
   *      - enqueue the event
   *    - otherwise:
   *      - send it
   *        - if successful
   *          - mark it as Delivered
   *        - if unsuccessful
   *          - create a queue in the map
   *          - enqueue the event into the queue
   */
  def subscribeToNewEvents: UIO[Any] = ZIO.unit

  // get events that are Delivering & AtLeastOnce
  // reconstruct webhookState
  def initiateRecovery: UIO[Any] = ZIO.unit

  // wait until all in progress work is finished
  def shutdown: IO[IOException, Any] = ZIO.unit
}

/**
 * We're providing a live layer for convenience and to ensure proper resource management.
 */
object WebhookServer {

  sealed trait WebhookState
  object WebhookState {
    case object Enabled                                                       extends WebhookState
    case object Disabled                                                      extends WebhookState
    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookEvent]) extends WebhookState
    case object Unavailable                                                   extends WebhookState
  }

  val live =
    (for {
      webhookState     <- Ref.makeManaged(Map.empty[WebhookId, WebhookServer.WebhookState])
      webhookRepo      <- ZManaged.service[WebhookRepo]
      webhookStateRepo <- ZManaged.service[WebhookStateRepo]
      webhookEventRepo <- ZManaged.service[WebhookEventRepo]
      server            = WebhookServer(webhookRepo, webhookStateRepo, webhookEventRepo, webhookState)
      _                <- server.start.orDie.toManaged_
      _                <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server).toLayer
}
