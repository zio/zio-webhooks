package zio.webhooks

import zio.webhooks.WebhookError._
import zio._
import zio.prelude.NonEmptySet
import zio.stream._

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Retrieves all events by their statuses.
   */
  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): UStream[WebhookEvent]

  /**
   * Retrieves events by [[WebhookId]] and a non-empty set of [[WebhookEventStatus]]es. Implementations should ensure
   * ordering of events.
   */
  def getEventsByWebhookAndStatus(
    id: WebhookId,
    statuses: NonEmptySet[WebhookEventStatus]
  ): Stream[MissingWebhookError, WebhookEvent]

  /**
   * Marks all events by the specified webhook id as failed.
   */
  def setAllAsFailedByWebhookId(webhookId: WebhookId): IO[MissingWebhookError, Unit]

  /**
   * Sets the status of the specified event.
   */
  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): IO[WebhookError, Unit]
}
