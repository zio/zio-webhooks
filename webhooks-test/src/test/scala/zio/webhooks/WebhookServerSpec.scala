package zio.webhooks

import zio._
import zio.duration._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test.TestAspect._
import zio.test._
import zio.webhooks.WebhookServerSpecHelper._
import zio.webhooks.testkit._
import zio.stream.ZStream

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      suite("when starting new event subscription")(
        testM("sends one event through for enabled webhooks") {
          val webhook = createWebhooks(1)(
            WebhookStatus.Enabled,
            WebhookDeliveryMode.SingleAtMostOnce
          ).head

          val webhookEvent = createWebhookEvent(1)(webhook.id).head
          for {
            _       <- TestWebhookRepo.createWebhook(webhook)
            _       <- TestWebhookEventRepo.createEvent(webhookEvent)
            queue   <- TestWebhookHttpClient.requests
            request <- queue.take
          } yield assert(request.url)(equalTo(webhook.url))
        },
        testM("can dispatch single event to n webhooks") {
          val n = 100

          val webhooks = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
          val events   = webhooks.flatMap(webhook => createWebhookEvent(1)(webhook.id))

          for {
            _     <- ZIO.foreach(webhooks)(TestWebhookRepo.createWebhook(_))
            _     <- ZIO.foreach(events)(TestWebhookEventRepo.createEvent(_))
            queue <- TestWebhookHttpClient.requests
            _     <- ZStream.fromQueue(queue).take(n.toLong).runCount
          } yield assertCompletes
        }
        // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
      )
    ).provideLayerShared(testEnv) @@ timeout(2.seconds)

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
