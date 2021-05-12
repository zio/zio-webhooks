package zio.webhooks

import zio.Chunk

/**
 * A [[WebhookEvent]] stores the content of a webhook event.
 */
final case class WebhookEvent(
  key: WebhookKey,
  status: WebhookEventStatus,
  content: String,
  headers: Chunk[(String, String)]
)
