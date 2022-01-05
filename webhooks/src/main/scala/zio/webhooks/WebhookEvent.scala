package zio.webhooks

import zio.Chunk

/**
 * A [[WebhookEvent]] stores the content of a webhook event.
 */
final case class WebhookEvent(
  key: WebhookEventKey,
  status: WebhookEventStatus,
  content: String,
  headers: Chunk[HttpHeader],
  token: Option[String]
) {

  lazy val contentType: Option[String] =
    headers.find(_._1.toLowerCase == "content-type").map(_._2)

  lazy val isDelivered: Boolean =
    status == WebhookEventStatus.Delivered

  lazy val isDelivering: Boolean =
    status == WebhookEventStatus.Delivering

  lazy val isDone: Boolean =
    status == WebhookEventStatus.Delivered || status == WebhookEventStatus.Failed

  lazy val isNew: Boolean =
    status == WebhookEventStatus.New

  lazy val webhookIdAndContentType: (WebhookId, Option[String]) =
    (key.webhookId, contentType)
}
