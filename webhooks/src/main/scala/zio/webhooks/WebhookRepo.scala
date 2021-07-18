package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
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
   * Polls webhooks by id for updates. Implementations must return a map of webhookIds to webhooks.
   */
  def pollWebhooksById(webhookIds: NonEmptySet[WebhookId]): UIO[Map[WebhookId, Webhook]]

  /**
   * Sets the status of a webhook with `id` to a new `status`.
   */
  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit]

  /**
   * Subscribes to webhook updates.
   */
  def subscribeToWebhookUpdates: UManaged[Dequeue[WebhookUpdate]]
}

object WebhookRepo {
  def subscribeToWebhooks: URManaged[Has[WebhookRepo], Dequeue[WebhookUpdate]] =
    ZManaged.service[WebhookRepo].flatMap(_.subscribeToWebhookUpdates)
}
