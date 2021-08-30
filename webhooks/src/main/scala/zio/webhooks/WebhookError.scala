package zio.webhooks

import zio.Cause

/**
 * Represents errors that can be raised during the operation of a webhook server.
 */
sealed trait WebhookError extends Exception with Product with Serializable { self =>
  override def getMessage: String =
    self match {
      case WebhookError.BadWebhookUrlError(badUrl, message)  =>
        s"""A webhook URL "$badUrl" failed to parse, cause: "$message""""
      case WebhookError.FatalError(cause)                    =>
        s"A fatal error occurred:\n$cause"
      case WebhookError.InvalidStateError(rawState, message) =>
        s"""Invalid state loaded on restart:\n$rawState\nwith message:\n"$message"""
    }
}

object WebhookError {

  /**
   * A [[BadWebhookUrlError]] occurs when a [[WebhookHttpClient]] detects a string that cannot be
   * parsed into a URL. The parsing failure is included as a message.
   */
  final case class BadWebhookUrlError(badUrl: String, message: String) extends WebhookError

  /**
   * A [[FatalError]] is raised when a [[WebhookServer]] dies due to a fatal error like a missing
   * webhook or a missing event.
   */
  final case class FatalError(cause: Cause[_]) extends WebhookError

  /**
   * An [[InvalidStateError]] occurs when decoding `rawState` during event recovery fails with a
   * `message`.
   */
  final case class InvalidStateError(rawState: String, message: String) extends WebhookError
}
