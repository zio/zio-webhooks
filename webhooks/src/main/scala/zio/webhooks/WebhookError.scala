package zio.webhooks

import zio.NonEmptyChunk

/**
 * Represents errors that can be raised during the operation of a webhook server.
 */
sealed trait WebhookError extends Product with Serializable
object WebhookError {

  /**
   * A [[MissingWebhookError]] occurs when a webhook we expect to exist is missing.
   */
  final case class MissingWebhookError(id: WebhookId) extends WebhookError

  /**
   * A [[MissingEventError]] occurs when a webhook event we expect to exist is missing.
   */
  final case class MissingEventError(key: WebhookEventKey) extends WebhookError

  /**
   * A [[MissingEventsError]] occurs when multiple events we expect to exist are missing.
   */
  final case class MissingEventsError(keys: NonEmptyChunk[WebhookEventKey]) extends WebhookError
}
