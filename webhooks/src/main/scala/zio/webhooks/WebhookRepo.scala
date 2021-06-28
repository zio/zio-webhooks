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

  final def requireWebhook(webhookId: WebhookId): IO[MissingWebhookError, Webhook] =
    getWebhookById(webhookId)
      .flatMap(ZIO.fromOption(_).orElseFail(MissingWebhookError(webhookId)))

  /**
   * Sets the status of a webhook.
   */
  def setWebhookStatus(id: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit]
}
