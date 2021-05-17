package zio.webhooks

import java.time.Instant

/**
 * A [[WebhookStatus]] describes the status of a webhook, which can be `Enabled`, `Disabled`,
 * `Retrying`, or `Unavailable`: the latter two being so since some [[java.time.Instant]].
 */
sealed trait WebhookStatus
object WebhookStatus {
  case object Enabled                              extends WebhookStatus
  case object Disabled                             extends WebhookStatus
  final case class Retrying(sinceTime: Instant)    extends WebhookStatus
  final case class Unavailable(sinceTime: Instant) extends WebhookStatus
}
