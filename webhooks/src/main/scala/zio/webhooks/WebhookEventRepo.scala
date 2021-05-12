package zio.webhooks

import zio.Task
import zio.stream.Stream

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Retrieves all events by their status.
   *
   * TODO: Change return type to stream
   */
  def getEventsByStatus(statuses: NonEmptySet[WebhookEventStatus]): Stream[Throwable, WebhookEvent]

  def getEventsByWebhookAndStatus(
    id: WebhookId,
    status: NonEmptySet[WebhookEventStatus]
  ): Stream[Throwable, WebhookEvent]

  /**
   * Sets the status of the specified event.
   */
  def setEventStatus(key: WebhookKey, status: WebhookEventStatus): Task[Unit]

  /**
   * Marks all events by the specified webhook id as failed.
   */
  def setAllAsFailedByWebhookId(webhookId: WebhookId): Task[Unit]
}
