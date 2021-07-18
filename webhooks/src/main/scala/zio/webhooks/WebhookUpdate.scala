package zio.webhooks

import zio.webhooks.WebhookUpdate._

sealed trait WebhookUpdate {
  final def status: Option[WebhookStatus] =
    this match {
      case WebhookRemoved(_)       =>
        None
      case WebhookChanged(webhook) =>
        Some(webhook.status)
    }
}

object WebhookUpdate {
  final case class WebhookRemoved(webhookId: WebhookId) extends WebhookUpdate
  final case class WebhookChanged(webhook: Webhook)     extends WebhookUpdate
}
