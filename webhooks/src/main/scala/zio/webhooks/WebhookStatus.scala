package zio.webhooks

import java.time.Instant

/**
 * A [[WebhookStatus]] describes the status of a webhook, which is either enabled or disabled, or
 * unavailable since a specified moment in time.
 */
sealed trait WebhookStatus
object WebhookStatus {
  case object Enabled                              extends WebhookStatus
  case object Disabled                             extends WebhookStatus
  final case class Retrying(sinceTime: Instant)    extends WebhookStatus
  final case class Unavailable(sinceTime: Instant) extends WebhookStatus
}
