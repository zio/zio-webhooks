package zio.webhooks

import zio.prelude.NonEmptySet

/**
 * Represents errors that can be raised during the operation of a webhook server.
 */
sealed trait WebhookError extends Product with Serializable
object WebhookError {

  /**
   * A [[BadWebhookUrlError]] occurs when a [[WebhookHttpClient]] detects a string that cannot be
   * parsed into a URL. The parsing failure is included as a message.
   */
  final case class BadWebhookUrlError(badUrl: String, message: String) extends WebhookError

  /**
   * An [[InvalidStateError]] occurs when decoding `rawState` during event recovery fails with a
   * `message`.
   */
  final case class InvalidStateError(rawState: String, message: String) extends WebhookError

  /**
   * A [[MissingWebhookError]] occurs when a webhook expected to exist is missing.
   */
  final case class MissingWebhookError(id: WebhookId) extends WebhookError

  /**
   * A [[MissingEventError]] occurs when a single webhook event expected to exist is missing.
   */
  final case class MissingEventError(key: WebhookEventKey) extends WebhookError

  /**
   * A [[MissingEventsError]] occurs when multiple events expected to exist are missing.
   */
  final case class MissingEventsError(keys: NonEmptySet[WebhookEventKey]) extends WebhookError
}
