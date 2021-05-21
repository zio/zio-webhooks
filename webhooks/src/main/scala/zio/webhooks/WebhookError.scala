package zio.webhooks

import zio.webhooks.WebhookError._

import java.io.IOException

/**
 * Represents errors that can be raised during the operation of a webhook server.
 */
sealed trait WebhookError extends Product with Serializable {
  def asThrowable: Throwable =
    this match {
      case MissingWebhookError(id)       => new Throwable(s"Missing webhook with id ${id.value}.")
      case MissingWebhookEventError(key) =>
        new Throwable(
          s"Missing webhook event with webhook id ${key.webhookId.value} and event id ${key.eventId.value}."
        )
      case IOError(e)                    => e
    }
}

object WebhookError {

  /**
   * A [[MissingWebhookError]] occurs when a webhook we expect to exist is missing.
   */
  case class MissingWebhookError(id: WebhookId) extends WebhookError

  /**
   * A [[MissingWebhookEventError]] occurs when a webhook event we expect to exist is missing.
   */
  case class MissingWebhookEventError(key: WebhookEventKey) extends WebhookError

  /**
   * An [[IOError]] occurs when an [[java.io.IOException]] is raised.
   */
  case class IOError(IOException: IOException) extends WebhookError
}
