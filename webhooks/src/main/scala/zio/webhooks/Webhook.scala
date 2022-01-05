package zio.webhooks

import zio.webhooks.WebhookStatus._

import java.time.Instant

/**
 * A [[Webhook]] represents a webhook (a web callback registered to receive notifications), and
 * contains an id, a URL, a label (used for diagnostic purposes), a status, and a delivery mode.
 */
final case class Webhook(
  id: WebhookId,
  url: String,
  label: String,
  status: WebhookStatus,
  deliveryMode: WebhookDeliveryMode,
  token: Option[String]
) {
  val batching: WebhookDeliveryBatching = deliveryMode.batching

  def disable: Webhook =
    copy(status = WebhookStatus.Disabled)

  def enable: Webhook =
    copy(status = WebhookStatus.Enabled)

  def isEnabled: Boolean =
    status == Enabled

  def markUnavailable(sinceTime: Instant): Webhook =
    copy(status = WebhookStatus.Unavailable(sinceTime))
}
