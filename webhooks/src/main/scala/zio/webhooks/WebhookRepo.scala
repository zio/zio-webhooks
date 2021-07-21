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
  def getWebhookById(webhookId: WebhookId): UIO[Option[Webhook]]

  /**
   * Sets the status of a webhook with `id` to a new `status`.
   */
  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit]
}
