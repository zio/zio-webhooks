package zio.webhooks.testkit

import java.io.IOException
import zio.{ Queue, Ref }
import zio.webhooks.WebhookHttpClient
import zio.Chunk
import zio.IO

final case class Request(url: String, content: String, headers: Chunk[(String, String)])
final case class Response(statusCode: Int)

final case class TestWebhookHttpClient(ref: Ref[Map[Request, Queue[Response]]]) extends WebhookHttpClient {

  def post(url: String, content: String, headers: Chunk[(String, String)]): IO[IOException, Unit] = ???
}
