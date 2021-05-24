package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks.WebhookError._

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Subscribes to events given a non-empty set of statuses. Implementations are responsible for
   * ordering events.
   */
  def subscribeToEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): UStream[WebhookEvent]

  /**
   * Retrieves events by [[WebhookId]] and a non-empty set of [[WebhookEventStatus]]es.
   * Implementations are responsible for ordering events.
   */
  def subscribeToEventsByWebhookAndStatus(
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
