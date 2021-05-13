package zio.webhooks

import WebhookError.MissingWebhookError
import zio.{ IO, Task }

/**
 * A [[WebhookRepo]] provides persistence facilities for webhooks.
 */
trait WebhookRepo {

  /**
   * Retrieves a webhook by id.
   */
  def getWebhookById(webhookId: WebhookId): Task[Option[Webhook]]

  /**
   * Sets the status of a webhook.
   */
  def setWebhookStatus(id: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit]
}
