package zio.webhooks

/**
 * A webhook event is identified by its [[WebhookEventId]], represented as a `Long`.
 */
final case class WebhookEventId(value: Long)
