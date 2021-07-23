package zio.webhooks.internal

import zio.Chunk
import zio.prelude.NonEmptySet
import zio.webhooks._

/**
 * A [[WebhookDispatch]] represents a unit of delivery to a [[Webhook]] containing a batch of one or
 * more [[WebhookEvent]]s.
 *
 * The server, when creating these dispatches, guarantees that all webhook events in this dispatch
 * will have the same [[WebhookEventContentType]].
 */
private[webhooks] final case class WebhookDispatch(
  webhookId: WebhookId,
  url: String,
  deliverySemantics: WebhookDeliverySemantics,
  events: NonEmptySet[WebhookEvent]
) {
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

  lazy val head: WebhookEvent                 = events.head
  lazy val headers: Chunk[(String, String)]   = events.head.headers
  lazy val keys: NonEmptySet[WebhookEventKey] = NonEmptySet.fromSet(events.head.key, events.tail.map(_.key))
  lazy val size: Int                          = events.size
}
