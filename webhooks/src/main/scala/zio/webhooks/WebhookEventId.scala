package zio.webhooks

import zio.json._

/**
 * A [[WebhookEvent]] is identified by its [[WebhookEventId]], represented as a `Long`.
 */
final case class WebhookEventId(value: Long)

object WebhookEventId {
  implicit val decoder: JsonDecoder[WebhookEventId] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[WebhookEventId] = DeriveJsonEncoder.gen
}
