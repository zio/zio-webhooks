package zio.webhooks.testkit

import java.io.IOException
import zio.{ Queue, Ref, ZIO }
import zio.webhooks.WebhookHttpClient
import zio._
import zio.webhooks.WebhookHttpRequest
import zio.webhooks.WebhookHttpResponse

trait TestWebhookHttpClient {
  def requests: UIO[Chunk[WebhookHttpRequest]]
}

object TestWebhookHttpClient {
  def requests: URIO[Has[TestWebhookHttpClient], Chunk[WebhookHttpRequest]] =
    ZIO.serviceWith(_.requests)

  val test: ULayer[Has[TestWebhookHttpClient] with Has[WebhookHttpClient]] = {
    for {
      ref      <- Ref.make(Map.empty[WebhookHttpRequest, Queue[WebhookHttpResponse]])
      received <- Ref.make[Chunk[WebhookHttpRequest]](Chunk.empty)
      impl      = TestWebhookHttpClientImpl(ref, received)
    } yield Has.allOf[TestWebhookHttpClient, WebhookHttpClient](impl, impl)
  }.toLayerMany
}

final case class TestWebhookHttpClientImpl(
  ref: Ref[Map[WebhookHttpRequest, Queue[WebhookHttpResponse]]],
  received: Ref[Chunk[WebhookHttpRequest]]
) extends WebhookHttpClient
    with TestWebhookHttpClient {

  def requests: UIO[Chunk[WebhookHttpRequest]] = received.get

  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse] =
    for {
      _        <- received.update(_ :+ request)
      map      <- ref.get
      queue    <- ZIO
                    .fromOption(map.get(request))
                    .mapError(_ => new IOException("No response for given request."))
      response <- queue.take
    } yield response
}
