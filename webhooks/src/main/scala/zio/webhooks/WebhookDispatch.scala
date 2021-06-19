package zio.webhooks

import zio.NonEmptyChunk
import zio.Chunk

/**
 * A [[WebhookDispatch]] represents a unit of delivery to a [[Webhook]] containing one or more
 * [[WebhookEvent]]s. Retries are done on dispatches since events can be batched.
 *
 * The server, when creating these dispatches, guarantees that all webhook events in this dispatch
 * will have the same [[WebhookEventContentType]].
 */
final case class WebhookDispatch private[webhooks] (webhook: Webhook, events: NonEmptyChunk[WebhookEvent]) {
  lazy val contentType: Option[WebhookEventContentType] =
    events.head.headers.find(_._1.toLowerCase == "content-type") match {
      case Some((_, contentTypeValue)) =>
        contentTypeValue match {
          case "text/plain"       =>
            Some(WebhookEventContentType.PlainText)
          case "application/json" =>
            Some(WebhookEventContentType.Json)
          case _                  =>
            None
        }
      case None                        =>
        None
    }

  lazy val head: WebhookEvent                   = events.head
  lazy val headers: Chunk[(String, String)]     = events.head.headers
  lazy val keys: NonEmptyChunk[WebhookEventKey] = events.map(_.key)
  lazy val semantics: WebhookDeliverySemantics  = webhook.deliveryMode.semantics
  lazy val size: Int                            = events.size
  lazy val url: String                          = webhook.url
}
