package zio.webhooks

import zio.IO
import zio.webhooks.WebhookError.BadWebhookUrlError
import zio.webhooks.WebhookHttpClient.HttpPostError

import java.io.IOException

/**
 * A [[WebhookHttpClient]] lets webhooks post data over HTTP.
 */
trait WebhookHttpClient {

  /**
   * [[WebhookHttpRequest]]s are sent over an HTTP POST method call.
   */
  def post(request: WebhookHttpRequest): IO[HttpPostError, WebhookHttpResponse]
}

object WebhookHttpClient {
  type HttpPostError = Either[BadWebhookUrlError, IOException]
}
