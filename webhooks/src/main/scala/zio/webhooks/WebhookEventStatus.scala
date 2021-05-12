package zio.webhooks

sealed trait WebhookEventStatus
object WebhookEventStatus {
  case object New        extends WebhookEventStatus
  case object Delivering extends WebhookEventStatus
  case object Delivered  extends WebhookEventStatus
  case object Failed     extends WebhookEventStatus
}
