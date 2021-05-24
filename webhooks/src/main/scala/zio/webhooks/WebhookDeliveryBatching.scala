package zio.webhooks

import java.time.Duration

/**
 * A [[Webhook]] can be delivered in two ways: as a `Single` or `Batched` delivery. `Single`
 * indicates that each [[WebhookEvent]] is delivered in one HTTP call. Conversely, `Batched`
 * indicates delivery of some batch of [[WebhookEvent]]s in one HTTP call for communication
 * efficiency.
 */
sealed trait WebhookDeliveryBatching extends Product with Serializable
object WebhookDeliveryBatching {
  case object Single extends WebhookDeliveryBatching

  // TODO: Smart constructor for `Batched`
  final case class Batched (size: Int, maxWait: Duration) extends WebhookDeliveryBatching
}
