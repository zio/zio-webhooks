package zio.webhooks

import zio._
import zio.prelude.NonEmptySet
import zio.stream.UStream

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Used by the server to recover events for webhooks with at-least-once delivery semantics with
   * state `Delivering`.
   */
  def recoverEvents: UStream[WebhookEvent]

  /**
   * Marks all events by the specified webhook id as failed.
   */
  def setAllAsFailedByWebhookId(webhookId: WebhookId): UIO[Unit]

  /**
   * Sets the status of an event.
   */
  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): UIO[Unit]

  /**
   * Sets the status of multiple events. Allows clients to specify custom logic to minimize overhead
   * when setting the event status of multiple events.
   */
  def setEventStatusMany(keys: NonEmptySet[WebhookEventKey], status: WebhookEventStatus): UIO[Unit]

  /**
   * Used by the server to subscribe to new webhook events.
   */
  def subscribeToNewEvents: URIO[Scope, Dequeue[WebhookEvent]]
}
