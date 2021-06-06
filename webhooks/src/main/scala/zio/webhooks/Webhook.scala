package zio.webhooks

import zio.webhooks.WebhookStatus._

/**
 * A [[Webhook]] represents a webhook (a web callback registered to receive notifications), and
 * contains an id, a URL, a label (used for diagnostic purposes), a status, and a delivery mode.
 */
final case class Webhook(
  id: WebhookId,
  url: String,
  label: String,
  status: WebhookStatus,
  deliveryMode: WebhookDeliveryMode
) {
  val batching: WebhookDeliveryBatching = deliveryMode.batching

  final def isOnline: Boolean =
    status match {
      case Enabled        => true
      case Retrying(_)    => true
      case Disabled       => false
      case Unavailable(_) => false
    }
}
