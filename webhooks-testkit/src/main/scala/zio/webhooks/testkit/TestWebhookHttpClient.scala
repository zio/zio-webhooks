package zio.webhooks.testkit

import java.io.IOException
import zio._
import zio.webhooks.WebhookHttpClient
import zio.webhooks.WebhookHttpRequest
import zio.webhooks.WebhookHttpResponse

// TODO: scaladoc
trait TestWebhookHttpClient {
  def requests: UIO[Queue[WebhookHttpRequest]]

  def setResponse(f: WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]): UIO[Unit]
}

object TestWebhookHttpClient {
  def requests: URIO[Has[TestWebhookHttpClient], Queue[WebhookHttpRequest]] =
    ZIO.serviceWith(_.requests)

  def setResponse(f: WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]): URIO[Has[TestWebhookHttpClient], Unit] =
    ZIO.serviceWith(_.setResponse(f))

  val test: ULayer[Has[TestWebhookHttpClient] with Has[WebhookHttpClient]] = {
    for {
      ref   <- Ref.makeManaged[WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]](_ => None)
      queue <- Queue.unbounded[WebhookHttpRequest].toManaged_
      impl   = TestWebhookHttpClientImpl(ref, queue)
    } yield Has.allOf[TestWebhookHttpClient, WebhookHttpClient](impl, impl)
  }.toLayerMany
}

final case class TestWebhookHttpClientImpl(
  ref: Ref[WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]],
  received: Queue[WebhookHttpRequest]
) extends WebhookHttpClient
    with TestWebhookHttpClient {

  def requests: UIO[Queue[WebhookHttpRequest]] = UIO(received)

  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse] =
    for {
      _        <- received.offer(request)
      f        <- ref.get
      queue    <- ZIO.fromOption(f(request)).mapError(_ => new IOException("No response set for given request."))
      response <- queue.take
    } yield response

  def setResponse(f: WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]): UIO[Unit] =
    ref.set(f)
}
