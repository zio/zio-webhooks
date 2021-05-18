package zio.webhooks.backends.sttp

import _root_.sttp.client._
import _root_.sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import _root_.sttp.model.Uri
import zio._
import zio.webhooks.WebhookHttpClient
import zio.webhooks.WebhookHttpRequest
import zio.webhooks.WebhookHttpResponse

import java.io.IOException

/**
 * A [[WebhookSttpClient]] provides a [[WebhookHttpClient]] backend which is in turn backed by
 * sttp's ZIO backend, specifically the [[AsyncHttpClientZioBackend]].
 */
final case class WebhookSttpClient(sttpClient: SttpClient) extends WebhookHttpClient {

  // TODO: should we use `Uri.safeApply` here instead? Is it on this lib to ensure proper URIs?
  // Right now we're failing fast when an invalid Uri is passed in.
  // If lib needs to ensure proper URIs, how do we change the signature?
  def post(webhookRequest: WebhookHttpRequest): IO[IOException, WebhookHttpResponse] = {
    // handrolled WebhookHttpRequest => sttp.client.RequestT
    val sttpRequest = basicRequest
      .post(Uri(webhookRequest.url))
      .body(webhookRequest.content)
      .headers(webhookRequest.headers.toMap)
    sttpClient
      .send(sttpRequest)
      .map(response => WebhookHttpResponse(response.code.code))
      .refineOrDie[IOException] { case e: IOException => e }
  }
}

object WebhookSttpClient {
  val live: TaskLayer[Has[WebhookHttpClient]] =
    AsyncHttpClientZioBackend.managed().map(WebhookSttpClient(_)).toLayer
}
