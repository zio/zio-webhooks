package zio.webhooks.testkit

import java.io.IOException
import zio.{ Queue, Ref, ZIO }
import zio.webhooks.WebhookHttpClient
import zio._
import zio.webhooks.WebhookHttpRequest
import zio.webhooks.WebhookHttpResponse

trait TestWebhookHttpClient {
  def requests: UIO[Queue[WebhookHttpRequest]]
}

object TestWebhookHttpClient {
  def requests: URIO[Has[TestWebhookHttpClient], Queue[WebhookHttpRequest]] =
    ZIO.serviceWith(_.requests)

  val test: ULayer[Has[TestWebhookHttpClient] with Has[WebhookHttpClient]] = {
    for {
      ref   <- Ref.makeManaged(Map.empty[WebhookHttpRequest, Queue[WebhookHttpResponse]])
      queue <- Queue.unbounded[WebhookHttpRequest].toManaged_
      impl   = TestWebhookHttpClientImpl(ref, queue)
    } yield Has.allOf[TestWebhookHttpClient, WebhookHttpClient](impl, impl)
  }.toLayerMany
}

final case class TestWebhookHttpClientImpl(
  ref: Ref[Map[WebhookHttpRequest, Queue[WebhookHttpResponse]]],
  received: Queue[WebhookHttpRequest]
) extends WebhookHttpClient
    with TestWebhookHttpClient {

  def requests: UIO[Queue[WebhookHttpRequest]] = ZIO.effectTotal(received)

  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse] =
    for {
      _        <- received.offer(request)
      map      <- ref.get
      queue    <- ZIO
                    .fromOption(map.get(request))
                    .mapError(_ => new IOException("No response for given request."))
      response <- queue.take
    } yield response
}
