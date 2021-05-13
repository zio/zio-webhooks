package zio.webhooks

/**
 * A [[WebhookEventKey]] represents a composite key that uniquely identifies a [[WebhookEvent]]
 * with respect to the [[Webhook]] it belongs to.
 */
final case class WebhookEventKey(eventId: WebhookEventId, webhookId: WebhookId)
