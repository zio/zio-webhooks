package zio.webhooks

import zio._

/**
 * A [[WebhookRepo]] provides persistence facilities for webhooks.
 */
trait WebhookRepo {

  /**
   * Retrieves a webhook by id.
   */
  def getWebhookById(webhookId: WebhookId): UIO[Webhook]

  /**
   * Sets the status of a webhook with `id` to a new `status`.
   */
  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): UIO[Unit]
}
