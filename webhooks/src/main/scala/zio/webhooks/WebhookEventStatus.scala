package zio.webhooks

/**
 * A [[WebhookEventStatus]] denotes the lifecycle of a [[WebhookEvent]]. Upon creation, an event is `New`. On
 * [[WebhookServer]] startup, it triggers a subscription which takes events and marks them as `Delivering`. Once the
 * server gets back a success from the webhook URL, the event is marked `Delivered`.
 */
sealed trait WebhookEventStatus
object WebhookEventStatus {
  case object New        extends WebhookEventStatus
  case object Delivering extends WebhookEventStatus
  case object Delivered  extends WebhookEventStatus
  case object Failed     extends WebhookEventStatus
}
