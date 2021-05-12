package zio.webhooks

sealed trait WebhookDeliveryBatching
object WebhookDeliveryBatching {
  case object Single  extends WebhookDeliveryBatching
  case object Batched extends WebhookDeliveryBatching
}
