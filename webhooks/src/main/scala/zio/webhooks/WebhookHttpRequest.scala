package zio.webhooks

import zio.Chunk

/**
 * A [[WebhookHttpRequest]] contains a subset of an HTTP request required to send webhook data.
 */
final case class WebhookHttpRequest private[webhooks] (url: String, content: String, headers: Chunk[(String, String)])
