package zio.webhooks

import zio.json._

/**
 * A [[WebhookEventKey]] represents a composite key that uniquely identifies a [[WebhookEvent]]
 * with respect to the [[Webhook]] it belongs to.
 */
final case class WebhookEventKey(eventId: WebhookEventId, webhookId: WebhookId)

object WebhookEventKey {
  implicit val decoder: JsonDecoder[WebhookEventKey] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[WebhookEventKey] = DeriveJsonEncoder.gen
}
