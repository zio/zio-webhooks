package zio.webhooks

import java.time.Instant

/**
 * A [[WebhookStatus]] describes the status of a webhook, which can be `Enabled`, `Disabled`, or
 * `Unavailable`: the last one being so since some [[java.time.Instant]]. Clients are responsible
 * for enabling and disabling webhooks as this is a read-only status as far as the webhook server is
 * concerned. The server sets a webhook unavailable when retries time out.
 */
sealed trait WebhookStatus extends Product with Serializable
object WebhookStatus {

  /**
   * An enabled webhook is one whose events are actively delivered.
   */
  case object Enabled extends WebhookStatus

  /**
   * A disabled webhook is one whose events are ignored for delivery.
   */
  case object Disabled extends WebhookStatus

  /**
   * An unavailable webhook signifies that retries for a webhook have timed out and that any new
   * events will be ignored for delivery.
   */
  final case class Unavailable(sinceTime: Instant) extends WebhookStatus
}
