package zio.webhooks

import zio.test.DefaultRunnableSpec
import zio.test._
import zio.webhooks.testkit._
import zio._
import zio.test.Assertion._

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      testM("can send one event through") {
        val webhook = Webhook(
          WebhookId(0),
          "http://httpbin.org/ip",
          "testWebhook",
          WebhookStatus.Enabled,
          WebhookDeliveryMode(WebhookDeliveryBatching.Single, WebhookDeliverySemantics.AtMostOnce)
        )

        val webhookEvent = WebhookEvent(
          WebhookEventKey(WebhookEventId(0), WebhookId(0)),
          WebhookEventStatus.New,
          "webhook event content",
          Chunk(("Accept", "*/*"))
        )

        for {
          _        <- TestWebhookRepo.createWebhook(webhook)
          _        <- TestWebhookEventRepo.createEvent(webhookEvent)
          requests <- TestWebhookHttpClient.requests
        } yield assert(requests.length)(equalTo(3))
      }
      // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
    ).provideLayerShared(testEnv)

  val testEnv: ULayer[TestEnv] = {
    val repos =
      (TestWebhookRepo.test >+> TestWebhookEventRepo.test) ++ TestWebhookStateRepo.test ++ TestWebhookHttpClient.test
    repos ++ (repos >>> WebhookServer.live)
  }.orDie

  type TestEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]
    with Has[WebhookServer]
}
