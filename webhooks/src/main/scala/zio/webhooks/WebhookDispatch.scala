package zio.webhooks

import zio.NonEmptyChunk
import zio.Chunk

/**
 * A [[Dispatch]] represents a unit of delivery to a [[Webhook]]. It can have one or more
 * [[WebhookEvent]]s. Retries are done on dispatches since status updates on webhooks and their
 * respective events depend on the outcome of each
 *
 * The server, when creating these dispatches, guarantees that all webhook events in this dispatch
 * will have the same [[WebhookEventContentType]].
 */
final case class WebhookDispatch(
  webhook: Webhook,
  events: NonEmptyChunk[WebhookEvent]
) {
  lazy val contentType: Option[WebhookEventContentType] =
    events.head.headers.find(_._1.toLowerCase == "content-type") match {
      case Some((_, contentTypeValue)) =>
        contentTypeValue match {
          case "text/plain"       => Some(WebhookEventContentType.PlainText)
          case "application/json" => Some(WebhookEventContentType.Json)
          case _                  => None
        }
      case None                        => None
    }

  lazy val head = events.head

  lazy val headers: Chunk[(String, String)] = events.head.headers

  lazy val keys: NonEmptyChunk[WebhookEventKey] = events.map(_.key)

  lazy val semantics = webhook.deliveryMode.semantics

  lazy val size: Int = events.size

  def toRequest: WebhookHttpRequest = {
    val requestContent =
      contentType match {
        case Some(WebhookEventContentType.Json) =>
          if (size > 1)
            "[" + events.map(_.content).mkString(",") + "]"
          else
            events.head.content
        case _                                  =>
          events.map(_.content).mkString
      }
    WebhookHttpRequest(url, requestContent, headers)
  }

  lazy val url: String = webhook.url
}
