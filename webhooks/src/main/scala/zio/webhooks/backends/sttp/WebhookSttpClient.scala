package zio.webhooks.backends.sttp

import _root_.sttp.client3.asynchttpclient.zio.{ AsyncHttpClientZioBackend, SttpClient }
import _root_.sttp.client3._
import sttp.model.Uri
import zio._
import zio.webhooks.WebhookError.BadWebhookUrlError
import zio.webhooks.WebhookHttpClient.HttpPostError
import zio.webhooks._

import java.io.IOException

/**
 * A [[WebhookSttpClient]] provides a [[WebhookHttpClient]] using sttp's ZIO backend, i.e.
 * `AsyncHttpClientZioBackend`.
 */
final case class WebhookSttpClient(sttpClient: SttpClient, permits: Semaphore) extends WebhookHttpClient {

  def post(webhookRequest: WebhookHttpRequest): IO[HttpPostError, WebhookHttpResponse] =
    permits.withPermit {
      for {
        url        <- ZIO
                        .fromEither(Uri.parse(webhookRequest.url))
                        .mapError(msg => Left(BadWebhookUrlError(webhookRequest.url, msg)))
        sttpRequest = basicRequest.post(url).body(webhookRequest.content).headers(webhookRequest.headers.toMap)
        response   <- sttpClient
                        .send(sttpRequest)
                        .map(response => WebhookHttpResponse(response.code.code))
                        .refineOrDie {
                          case e: SttpClientException   => e
                          case e: IllegalStateException => e // capture request errors made after shutting down
                        }
                        .mapError(e => Right(new IOException(e.getMessage)))
      } yield response
    }
}

object WebhookSttpClient {

  /**
   * A layer built with an STTP ZIO backend with the default settings
   */
  val live: RLayer[WebhookServerConfig, WebhookHttpClient] = {
    for {
      sttpBackend <- AsyncHttpClientZioBackend.managed()
      capacity    <- ZManaged.service[WebhookServerConfig].map(_.maxRequestsInFlight)
      permits     <- Semaphore.make(capacity.toLong).toManaged
    } yield WebhookSttpClient(sttpBackend, permits)
  }.toLayer
}
