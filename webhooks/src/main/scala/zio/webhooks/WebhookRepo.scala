package zio.webhooks

import zio.Task

/**
 * A [[WebhookRepo]] provides persistence facilities for webhooks.
 */
trait WebhookRepo {
  def getWebhookById(webhookId: WebhookId): Task[Option[Webhook]]

  def setWebhookStatus(id: WebhookId, status: WebhookStatus): Task[Unit]
}
