package zio.webhooks

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
}
