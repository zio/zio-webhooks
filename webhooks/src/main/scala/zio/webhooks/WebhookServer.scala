package zio.webhooks

import zio._
import java.io.IOException
import java.time.Instant
import zio.prelude.NonEmptySet

final case class WebhookServer(
  webhookRepo: WebhookRepo,
  stateRepo: WebhookStateRepo,
  eventRepo: WebhookEventRepo,
  httpClient: WebhookHttpClient,
  // consider STM if needed
  webhookState: Ref[Map[WebhookId, WebhookServer.WebhookState]]
) {

  /**
   * Starts the webhook server. Kicks off the following to run concurrently:
   * - new webhook event subscription
   * - event recovery for events
   * - retry monitoring
   */
  def start: IO[IOException, Any] =
    // start fibers for each
    startEventSubscription *> startEventRecovery *> startRetryMonitoring

  // periodically retry every WebhookEvent in queue
  // if we've retried for >7 days,
  //   Webhook as Unavailable
  //   clear WebhookState for that Webhook
  //   make sure that subscribeToNewEvents does _not_ try to deliver to an unavailable webhook
  /**
   * Kicks off periodic retries for every [[WebhookEvent]] pending delivery.
   */
  private def startRetryMonitoring: UIO[Any] = ZIO.unit // ticket

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
  /**
   * Kicks off new [[WebhookEvent]] subscription.
   */
  private def startEventSubscription: UIO[Any] =
    eventRepo
      .getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .foreach(newEvent =>
        for {
          opt     <- webhookRepo.getWebhookById(newEvent.key.webhookId)
          // how to handle domain errors here?
          webhook <- ZIO.fromOption(opt) // for now
          request  = WebhookHttpRequest(webhook.url, newEvent.content, newEvent.headers)
          _       <- httpClient.post(request)
        } yield ()
      )
      .ignore
      .fork // keep track of fibers for when we shut down?

  // get events that are Delivering & AtLeastOnce
  // reconstruct webhookState
  /**
   * Kicks off recovery of events that with the following delivery mode: `Delivering` & `AtLeastOnce`.
   */
  private def startEventRecovery: UIO[Any] = ZIO.unit

  // Maybe should have state somewhere here shut down.
  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit
}

/**
 * We're providing a live layer for convenience and to ensure proper resource management.
 */
object WebhookServer {
  type Env = Has[WebhookRepo] with Has[WebhookStateRepo] with Has[WebhookEventRepo] with Has[WebhookHttpClient]

  sealed trait WebhookState
  object WebhookState {
    case object Enabled                                                       extends WebhookState
    case object Disabled                                                      extends WebhookState
    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookEvent]) extends WebhookState
    case object Unavailable                                                   extends WebhookState
  }

  val live: RLayer[WebhookServer.Env, Has[WebhookServer]] =
    (for {
      state      <- Ref.makeManaged(Map.empty[WebhookId, WebhookServer.WebhookState])
      repo       <- ZManaged.service[WebhookRepo]
      stateRepo  <- ZManaged.service[WebhookStateRepo]
      eventRepo  <- ZManaged.service[WebhookEventRepo]
      httpClient <- ZManaged.service[WebhookHttpClient]
      server      = WebhookServer(repo, stateRepo, eventRepo, httpClient, state)
      _          <- server.start.orDie.toManaged_
      _          <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server).toLayer
}
