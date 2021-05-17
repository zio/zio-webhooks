package zio.webhooks

import zio.IO
import WebhookError.SendWebhookEventError

/**
 * A [[WebhookHttpClient]] provides the facility to send a [[WebhookEvent]] over HTTP.
 */
trait WebhookHttpClient {
  def sendWebhookEvent(webhookEvent: WebhookEvent): IO[SendWebhookEventError, Unit]
}
