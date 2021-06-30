package zio.webhooks

import zio.Chunk

/**
 * A [[WebhookEvent]] stores the content of a webhook event.
 */
final case class WebhookEvent(
  key: WebhookEventKey,
  status: WebhookEventStatus,
  content: String,
  headers: Chunk[(String, String)]
) {
  lazy val isDelivered: Boolean = status == WebhookEventStatus.Delivered

  lazy val webhookIdAndContentType: (WebhookId, Option[(String, String)]) =
    (key.webhookId, headers.find(_._1.toLowerCase == "content-type"))
}
