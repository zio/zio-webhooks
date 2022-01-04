package zio.webhooks.internal

import zio.Chunk
import zio.prelude.NonEmptySet
import zio.webhooks._

/**
 * A [[WebhookDispatch]] represents a unit of delivery to a [[Webhook]] containing a payload of one
 * or more [[WebhookEvent]]s.
 *
 * The server, when creating these dispatches, guarantees that all webhook events in this dispatch
 * will have the same content type.
 */
private[webhooks] final case class WebhookDispatch(
  webhookId: WebhookId,
  url: String,
  deliverySemantics: WebhookDeliverySemantics,
  payload: WebhookPayload
) {
  lazy val contentType: Option[WebhookContentMimeType] =
    headers.collectFirst {
      case (headerName, header) if headerName.toLowerCase == "content-type" =>
        WebhookContentMimeType(header)
    }

  lazy val events: NonEmptySet[WebhookEvent] =
    payload match {
      case WebhookPayload.Single(event)   =>
        NonEmptySet.single(event)
      case WebhookPayload.Batched(events) =>
        events
    }

  lazy val headers: Chunk[HttpHeader] = payload.headers
}
