package zio.webhooks.testkit

import java.io.IOException
import zio.{ Queue, Ref, ZIO }
import zio.webhooks.WebhookHttpClient
import zio.IO
import zio.webhooks.WebhookHttpRequest
import zio.webhooks.WebhookHttpResponse

final case class TestWebhookHttpClient(ref: Ref[Map[WebhookHttpRequest, Queue[WebhookHttpResponse]]])
    extends WebhookHttpClient {

  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse] =
    for {
      map      <- ref.get
      queue    <- ZIO
                    .fromOption(map.get(request))
                    .mapError(_ => new IOException("No response for given request."))
      response <- queue.take
    } yield response
}
