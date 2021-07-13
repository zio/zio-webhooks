package zio.webhooks

import zio._
import zio.stream.UStream
import zio.webhooks.WebhookError._

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Used by the server to recover events for webhooks with at-least-once delivery semantics.
   */
  def recoverEvents: UStream[WebhookEvent]

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

  /**
   * Used by the server to subscribe to new webhook events.
   */
  def subscribeToNewEvents: UManaged[Dequeue[WebhookEvent]]
}
