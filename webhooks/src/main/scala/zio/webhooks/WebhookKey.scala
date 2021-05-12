package zio.webhooks

final case class WebhookKey(eventId: WebhookEventId, webhookId: WebhookId)
