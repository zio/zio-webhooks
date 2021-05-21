package zio.webhooks

import zio._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test._
import zio.webhooks.WebhookServerSpecHelper._
import zio.webhooks.testkit._

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      testM("sends one event through for enabled webhooks") {
        val webhook = Webhook(
          WebhookId(0),
          "http://foo.bar",
          "testWebhook",
          WebhookStatus.Enabled,
          WebhookDeliveryMode.SingleAtMostOnce
        )

        val webhookEvent = WebhookEvent(
          WebhookEventKey(WebhookEventId(0L), webhook.id),
          WebhookEventStatus.New,
          "lorem ipsum",
          Chunk(("Accept", "*/*"))
        )

        for {
          _     <- TestWebhookRepo.createWebhook(webhook)
          _     <- TestWebhookEventRepo.createEvent(webhookEvent)
          queue <- TestWebhookHttpClient.requests
          _     <- queue.take
        } yield assertCompletes
      },
      testM("can dispatch single event to n webhooks") {
        val n        = 100
        val webhooks = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
        val events   = webhooks.flatMap(webhook => createWebhookEvent(1)(webhook.id))

        for {
          queue <- Queue.bounded[WebhookHttpResponse](n)
          _     <- queue.offerAll(Chunk.fill(n)(WebhookHttpResponse(200)))
          _     <- TestWebhookHttpClient.setResponse(_ => Some(queue))
          _     <- ZIO.foreach(webhooks)(TestWebhookRepo.createWebhook(_))
          _     <- ZIO.foreach(events)(TestWebhookEventRepo.createEvent(_))
          queue <- TestWebhookHttpClient.requests
          size  <- queue.takeN(100).map(_.size)
        } yield assert(size)(equalTo(100))
      }
      // TODO: test that errors in the subscription crash the server?
      // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
    ).provideLayer(testEnv)

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
