package zio.webhooks.backends.sttp

import _root_.sttp.client._
import _root_.sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import sttp.model.Uri
import zio._
import zio.webhooks.WebhookError.BadWebhookUrlError
import zio.webhooks.WebhookHttpClient.HttpPostError
import zio.webhooks.{ WebhookHttpClient, WebhookHttpRequest, WebhookHttpResponse }

import java.io.IOException

/**
 * A [[WebhookSttpClient]] provides a [[WebhookHttpClient]] using sttp's ZIO backend, i.e.
 * [[sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend]].
 */
final case class WebhookSttpClient(sttpClient: SttpClient) extends WebhookHttpClient {

  def post(webhookRequest: WebhookHttpRequest): IO[HttpPostError, WebhookHttpResponse] =
    for {
      url        <- ZIO
                      .fromEither(Uri.parse(webhookRequest.url))
                      .mapError(msg => Left(BadWebhookUrlError(webhookRequest.url, msg)))
      sttpRequest = basicRequest.post(url).body(webhookRequest.content).headers(webhookRequest.headers.toMap)
      response   <- sttpClient
                      .send(sttpRequest)
                      .map(response => WebhookHttpResponse(response.code.code))
                      .refineToOrDie[IOException]
                      .mapError(Right(_))
    } yield response
}

object WebhookSttpClient {
  val live: TaskLayer[Has[WebhookHttpClient]] =
    AsyncHttpClientZioBackend.managed().map(WebhookSttpClient(_)).toLayer
}
