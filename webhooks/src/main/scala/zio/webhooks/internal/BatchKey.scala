package zio.webhooks.internal

import zio.webhooks.WebhookId

/**
 * A [[BatchKey]] specifies how the server groups events for batching: by [[WebhookId]] and content
 * type.
 */
private[webhooks] final case class BatchKey(webhookId: WebhookId, contentType: Option[String])
