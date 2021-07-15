package zio.webhooks

import zio._
import zio.webhooks.WebhookError.MissingWebhookError

/**
 * A [[WebhookRepo]] provides persistence facilities for webhooks.
 */
trait WebhookRepo {

  /**
   * Retrieves a webhook by id.
   */
  def getWebhookById(webhookId: WebhookId): IO[MissingWebhookError, Webhook]

  /**
   * Sets the status of a webhook.
   */
  def setWebhookStatus(id: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit]
}
