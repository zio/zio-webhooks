package zio.webhooks

/**
 * A [[Webhook]] represents a webhook (a web callback registered to receive notifications), and
 * contains an id, a URL, a label (used for diagnostic purposes), a status, and an optional secret
 * token, which is used for signing events.
 */
final case class Webhook(
  id: WebhookId,
  url: String,
  label: String,
  status: WebhookStatus,
  deliveryMode: WebhookDeliveryMode
)
