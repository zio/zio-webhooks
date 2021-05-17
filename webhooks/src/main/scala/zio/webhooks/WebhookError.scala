package zio.webhooks

/**
 * Represents errors that can be raised during the operation of a webhook server.
 */
sealed trait WebhookError
object WebhookError {

  /**
   * A [[SendWebhookEventError]] occurs when a webhook event fails to get sent.
   */
  case class SendWebhookEventError(webhookEvent: WebhookEvent, message: String) extends WebhookError
}

sealed trait MissingWebhookObjectError extends WebhookError
object MissingWebhookObjectError {

  /**
   * A [[MissingWebhookError]] occurs when a webhook with the given [[WebhookId]] we expect to exist
   * is missing.
   */
  final case class MissingWebhookError(id: WebhookId) extends MissingWebhookObjectError

  /**
   * A [[MissingWebhookEventError]] occurs when a webhook event with the given [[WebhookEventKey]]
   * we expect to exist is missing.
   */
  final case class MissingWebhookEventError(key: WebhookEventKey) extends MissingWebhookObjectError
}
