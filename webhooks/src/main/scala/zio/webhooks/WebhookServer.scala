package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
import zio.webhooks.WebhookError._

import java.io.IOException
import java.time.Instant

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
  def start: IO[WebhookError, Any] =
    startNewEventSubscription *> startEventRecovery *> startRetryMonitoring

  // get events that are Delivering & AtLeastOnce
  // reconstruct webhookState
  /**
   * Kicks off recovery of events that with the following delivery mode: `Delivering` & `AtLeastOnce`.
   */
  private def startEventRecovery: IO[WebhookError, Any] = ZIO.unit

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
  private def startNewEventSubscription: IO[WebhookError, Any] =
    eventRepo
      .getEventsByStatuses(NonEmptySet(WebhookEventStatus.New))
      .foreach { newEvent =>
        val webhookId = newEvent.key.webhookId
        for {
          webhook <- webhookRepo
                       .getWebhookById(webhookId)
                       .flatMap(opt => ZIO.fromOption(opt).mapError(_ => MissingWebhookError(webhookId)))
          request  = WebhookHttpRequest(webhook.url, newEvent.content, newEvent.headers)
          // TODO: do something with response
          // TODO: write test to handle failure?
          _       <- ZIO.unless(webhook.isDisabled)(httpClient.post(request).ignore)
        } yield () // should WebhookError crash server?
      }
      .fork

  // periodically retry every WebhookEvent in queues
  // if we've retried for >7 days,
  //   Webhook as Unavailable
  //   clear WebhookState for that Webhook
  //   make sure that subscribeToNewEvents does _not_ try to deliver to an unavailable webhook
  /**
   * Kicks off backoff retries for every [[WebhookEvent]] pending delivery.
   */
  private def startRetryMonitoring: IO[WebhookError, Any] = ZIO.unit // ticket

  /**
   * Waits until all work in progress is finished, then shuts down.
   */
  def shutdown: IO[IOException, Any] = ZIO.unit
}

object WebhookServer {
  type Env = Has[WebhookRepo] with Has[WebhookStateRepo] with Has[WebhookEventRepo] with Has[WebhookHttpClient]

  sealed trait WebhookState
  object WebhookState {
    case object Enabled                                                       extends WebhookState
    case object Disabled                                                      extends WebhookState
    final case class Retrying(sinceTime: Instant, queue: Queue[WebhookEvent]) extends WebhookState
    case object Unavailable                                                   extends WebhookState
  }

  /**
   * A live server layer is provided for convenience and to ensure proper resource management.
   */
  val live: RLayer[WebhookServer.Env, Has[WebhookServer]] = {
    for {
      state      <- Ref.makeManaged(Map.empty[WebhookId, WebhookServer.WebhookState])
      repo       <- ZManaged.service[WebhookRepo]
      stateRepo  <- ZManaged.service[WebhookStateRepo]
      eventRepo  <- ZManaged.service[WebhookEventRepo]
      httpClient <- ZManaged.service[WebhookHttpClient]
      server      = WebhookServer(repo, stateRepo, eventRepo, httpClient, state)
      _          <- server.start.mapError(_.asThrowable).orDie.toManaged_
      _          <- ZManaged.finalizer(server.shutdown.orDie)
    } yield server
  }.toLayer
}
