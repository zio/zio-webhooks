package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
import zio.webhooks.WebhookError._

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Used by the server to subscribe to events whose status is in the given set of `statuses`.
   */
  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): UManaged[Dequeue[WebhookEvent]]

  /**
   * Used by the server during event recovery to get a webhook's events whose status is in the given
   * set of `statuses`.
   */
  def getEventsByWebhookAndStatus(
    id: WebhookId,
    statuses: NonEmptySet[WebhookEventStatus]
  ): UIO[Chunk[WebhookEvent]]

  /**
   * Marks all events by the specified webhook id as failed.
   */
  def setAllAsFailedByWebhookId(webhookId: WebhookId): IO[MissingEventsError, Unit]

  /**
   * Sets the status of the specified event.
   */
  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): IO[MissingEventError, Unit]

  /**
   * Sets the status of multiple events.
   */
  def setEventStatusMany(keys: NonEmptyChunk[WebhookEventKey], status: WebhookEventStatus): IO[MissingEventsError, Unit]
}
