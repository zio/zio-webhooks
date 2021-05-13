package zio.webhooks

/**
 * Represents errors that can be raised during the operation of a webhook server.
 */
sealed trait WebhookError
object WebhookError {

  /**
   * A [[MissingWebhookError]] occurs when a webhook we expect to exist is missing.
   */
  case class MissingWebhookError(id: WebhookId) extends WebhookError
}
