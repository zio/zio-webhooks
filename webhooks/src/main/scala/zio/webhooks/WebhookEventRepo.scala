package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
import zio.webhooks.WebhookError._

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Subscribes to events given a non-empty set of statuses. Implementations are responsible for
   * ordering events.
   */
  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): UManaged[Dequeue[WebhookEvent]]

  /**
   * Retrieves events by [[WebhookId]] and a non-empty set of [[WebhookEventStatus]]es.
   * Implementations are responsible for ordering events.
   */
  def getEventsByWebhookAndStatus(
    id: WebhookId,
    statuses: NonEmptySet[WebhookEventStatus]
  ): UManaged[Dequeue[WebhookEvent]]

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
