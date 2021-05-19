package zio.webhooks

import zio.test.DefaultRunnableSpec
import zio.test._
import zio.webhooks.testkit._
import zio._
import zio.test.Assertion._
// import zio.duration.Duration
// import zio.test.environment.TestClock

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
          // ⚠🚨 Temporary hack 🚨⚠ This works, but we're waiting for the server to do stuff.
          // TODO: Change TestWebhookHttpClient#requests so it exposes a Queue/Stream for us to listen to instead.
          _         = Thread.sleep(125)
          requests <- TestWebhookHttpClient.requests
        } yield assert(requests.length)(equalTo(1))
      }
      // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
    ).provideLayerShared(testEnv)

  type TestEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]
    with Has[WebhookServer]

  val testEnv: ULayer[TestEnv] = {
    val repos =
      (TestWebhookRepo.test >+> TestWebhookEventRepo.test) ++ TestWebhookStateRepo.test ++ TestWebhookHttpClient.test
    repos ++ (repos >>> WebhookServer.live)
  }.orDie
}
