package zio.webhooks.testkit

import zio._
import zio.stream.UStream
import zio.webhooks._

import java.io.IOException
import zio.stream.ZStream

// TODO: scaladoc
trait TestWebhookHttpClient {
  def requests: UStream[WebhookHttpRequest]

  def setResponse(f: WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]): UIO[Unit]
}

object TestWebhookHttpClient {
  // Accessors

  def requests: ZStream[Has[TestWebhookHttpClient], Nothing, WebhookHttpRequest] =
    ZStream.service[TestWebhookHttpClient].flatMap(_.requests)

  def setResponse(f: WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]): URIO[Has[TestWebhookHttpClient], Unit] =
    ZIO.serviceWith(_.setResponse(f))

  val test: ULayer[Has[TestWebhookHttpClient] with Has[WebhookHttpClient]] = {
    for {
      ref   <- Ref.makeManaged[WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]](_ => None)
      queue <- Hub.unbounded[WebhookHttpRequest].toManaged_
      impl   = TestWebhookHttpClientImpl(ref, queue)
    } yield Has.allOf[TestWebhookHttpClient, WebhookHttpClient](impl, impl)
  }.toLayerMany
}

final case class TestWebhookHttpClientImpl(
  ref: Ref[WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]],
  received: Hub[WebhookHttpRequest]
) extends WebhookHttpClient
    with TestWebhookHttpClient {

  def requests: UStream[WebhookHttpRequest] =
    UStream.fromHub(received)

  def post(request: WebhookHttpRequest): IO[IOException, WebhookHttpResponse] =
    for {
      _        <- received.publish(request)
      f        <- ref.get
      queue    <- ZIO.fromOption(f(request)).mapError(_ => new IOException("No response set for given request."))
      response <- queue.take
    } yield response

  def setResponse(f: WebhookHttpRequest => Option[Queue[WebhookHttpResponse]]): UIO[Unit] =
    ref.set(f)
}
