package zio.webhooks

import zio.json._

/**
 * A webhook is identified by a `Long`.
 */
final case class WebhookId(value: Long)

object WebhookId {
  implicit val decoder: JsonDecoder[WebhookId] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[WebhookId] = DeriveJsonEncoder.gen
}
