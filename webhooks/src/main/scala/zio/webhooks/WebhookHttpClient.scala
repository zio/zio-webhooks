package zio.webhooks

import zio.IO
import java.io.IOException

/**
 * A [[WebhookHttpClient]] provides the facility to post data over HTTP.
 */
trait WebhookHttpClient {
  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse]
}
