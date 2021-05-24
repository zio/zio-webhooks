package zio.webhooks

/**
 * A [[Webhook]] represents a webhook (a web callback registered to receive notifications), and
 * contains an id, a URL, a label (used for diagnostic purposes), a status, and a delivery mode
 */
final case class Webhook(
  id: WebhookId,
  url: String,
  label: String,
  status: WebhookStatus,
  deliveryMode: WebhookDeliveryMode
) {
  final def isDisabled: Boolean =
    status == WebhookStatus.Disabled
}
