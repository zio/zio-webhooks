package zio.webhooks.testkit

import zio._
import zio.webhooks.WebhookError.BadWebhookUrlError
import zio.webhooks.WebhookHttpClient.HttpPostError
import zio.webhooks._
import zio.webhooks.testkit.TestWebhookHttpClient.StubResponses

import java.io.IOException

// TODO: scaladoc
trait TestWebhookHttpClient {
  def requests: URIO[Scope, Dequeue[WebhookHttpRequest]]

  def setResponse(f: WebhookHttpRequest => StubResponses): UIO[Unit]
}

object TestWebhookHttpClient {
  // Accessors

  def getRequests: URIO[Scope with TestWebhookHttpClient, Dequeue[WebhookHttpRequest]] =
    ZIO.serviceWithZIO[TestWebhookHttpClient](_.requests)

  def setResponse(
    f: WebhookHttpRequest => StubResponses
  ): URIO[TestWebhookHttpClient, Unit] =
    ZIO.serviceWithZIO(_.setResponse(f))

  val test: ULayer[TestWebhookHttpClient with WebhookHttpClient] = ZLayer.scoped {
    for {
      ref   <- Ref.make[WebhookHttpRequest => StubResponses](_ => None)
      queue <- Hub.unbounded[WebhookHttpRequest]
      impl   = TestWebhookHttpClientImpl(ref, queue)
    } yield impl
  }

  type StubResponse  = Either[Option[BadWebhookUrlError], WebhookHttpResponse]
  type StubResponses = Option[Queue[StubResponse]]
}

final case class TestWebhookHttpClientImpl(
  ref: Ref[WebhookHttpRequest => StubResponses],
  received: Hub[WebhookHttpRequest]
) extends WebhookHttpClient
    with TestWebhookHttpClient {

  def requests: URIO[Scope, Dequeue[WebhookHttpRequest]] =
    received.subscribe

  def post(request: WebhookHttpRequest): IO[HttpPostError, WebhookHttpResponse] =
    for {
      _        <- received.publish(request)
      f        <- ref.get
      queue    <- ZIO.fromOption(f(request)).orElseFail(Right(new IOException("No response set for given request.")))
      response <- queue.take.absolve.mapError(_.toLeft(new IOException("Query failed")))
    } yield response

  def setResponse(f: WebhookHttpRequest => StubResponses): UIO[Unit] =
    ref.set(f)
}
