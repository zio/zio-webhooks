package zio.webhooks

/**
 * A [[WebhookEvent]] is identified by its [[WebhookEventId]], represented as a `Long`.
 */
final case class WebhookEventId(value: Long)
