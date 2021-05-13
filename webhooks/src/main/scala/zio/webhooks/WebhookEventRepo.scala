package zio.webhooks

import zio.Task
import zio.prelude.NonEmptySet
import zio.stream.Stream

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Retrieves all events by their statuses.
   */
  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): Stream[Throwable, WebhookEvent]

  /**
   * Retrieves events by [[WebhookId]] and a non-empty set of [[WebhookEventStatus]]es. Implementations should ensure
   * ordering of events.
   */
  def getEventsByWebhookAndStatus(
    id: WebhookId,
    status: NonEmptySet[WebhookEventStatus]
  ): Stream[Throwable, WebhookEvent]

  /**
   * Sets the status of the specified event.
   */
  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): Task[Unit]

  /**
   * Marks all events by the specified webhook id as failed.
   */
  def setAllAsFailedByWebhookId(webhookId: WebhookId): Task[Unit]
}
