package zio.webhooks

import zio.webhooks.WebhookUpdate._

/**
 * A [[WebhookUpdate]] represents the removal of or a change to a [[Webhook]]. A [[WebhookServer]]
 * can subscribe to these to incrementally update its internal webhook map.
 */
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
