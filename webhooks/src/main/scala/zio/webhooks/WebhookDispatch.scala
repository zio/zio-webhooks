package zio.webhooks

import zio.NonEmptyChunk
import zio.Chunk

/**
 * A [[Dispatch]] represents a unit of delivery to a [[Webhook]]. It can have one or more
 * [[WebhookEvent]]s. Retries are done on dispatches since status updates on webhooks and their
 * respective events depend on the outcome of each dispatch.
 *
 * The server, when creating these dispatches, guarantees that all webhook events in this dispatch
 * will have the same [[WebhookEventContentType]].
 */
final case class WebhookDispatch(
  webhook: Webhook,
  events: NonEmptyChunk[WebhookEvent]
) {
  val contentType: Option[WebhookEventContentType] =
    events.head.headers.find(_._1.toLowerCase == "content-type") match {
      case Some((_, contentTypeValue)) =>
        contentTypeValue match {
          case "text/plain"       => Some(WebhookEventContentType.PlainText)
          case "application/json" => Some(WebhookEventContentType.Json)
          case _                  => None
        }
      case None                        => None
    }

  val headers: Chunk[(String, String)] = events.head.headers

  val size: Int = events.size

  val url: String = webhook.url
}
