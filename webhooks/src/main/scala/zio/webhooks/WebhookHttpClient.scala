package zio.webhooks

import zio.IO
import java.io.IOException

/**
 * A [[WebhookHttpClient]] lets webhooks post data over HTTP.
 */
trait WebhookHttpClient {

  /**
    * Webhooks send a [[WebhookHttpRequest]] as a POST method.
    */
  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse]
}
