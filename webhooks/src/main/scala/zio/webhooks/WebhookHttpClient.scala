package zio.webhooks

import zio.IO
import zio.Chunk
import java.io.IOException

/**
 * A [[WebhookHttpClient]] provides the facility to post data over HTTP.
 */
trait WebhookHttpClient {
  def post(url: String, content: String, headers: Chunk[(String, String)]): IO[IOException, Unit]
}
