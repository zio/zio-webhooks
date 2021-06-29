package zio.webhooks

import zio.json._

import java.time.Instant

/**
 * A [[WebhookStatus]] describes the status of a webhook, which can be `Enabled`, `Disabled`,
 * `Retrying`, or `Unavailable`: the latter two being so since some [[java.time.Instant]].
 */
sealed trait WebhookStatus extends Product with Serializable
object WebhookStatus {
  case object Enabled                              extends WebhookStatus
  case object Disabled                             extends WebhookStatus
  final case class Retrying(sinceTime: Instant)    extends WebhookStatus
  final case class Unavailable(sinceTime: Instant) extends WebhookStatus

  implicit val decoder: JsonDecoder[WebhookStatus] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[WebhookStatus] = DeriveJsonEncoder.gen
}
