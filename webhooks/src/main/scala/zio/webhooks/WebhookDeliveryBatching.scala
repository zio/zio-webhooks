package zio.webhooks

/**
 * [[Webhook]] delivery comes in two modes: `Single` or `Batched` delivery. `Single` indicates that
 * each [[WebhookEvent]] is delivered in one HTTP call. Conversely, `Batched` indicates delivery of
 * some batch of [[WebhookEvent]]s in one HTTP call for communication efficiency.
 */
sealed trait WebhookDeliveryBatching extends Product with Serializable
object WebhookDeliveryBatching {
  case object Single  extends WebhookDeliveryBatching
  case object Batched extends WebhookDeliveryBatching
}
