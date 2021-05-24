package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live
import zio.webhooks.WebhookServerSpecHelper._
import zio.webhooks.testkit._

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      suite("on server new event subscription")(
        testM("can dispatch single event to n webhooks") {
          val n        = 100
          val webhooks = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
          val events   = webhooks.flatMap(webhook => createWebhookEvent(1)(webhook.id))

          for {
            stubResponses <- Queue.unbounded[WebhookHttpResponse]
            _             <- stubResponses.offerAll(Chunk.fill(n)(WebhookHttpResponse(200)))
            _             <- TestWebhookHttpClient.setResponse(_ => Some(stubResponses))
            _             <- ZIO.foreach(webhooks)(TestWebhookRepo.createWebhook(_))
            _             <- ZIO.foreach(events)(TestWebhookEventRepo.createEvent(_))
            requestsMade  <- TestWebhookHttpClient.requests
            size          <- requestsMade.takeN(n).map(_.size)
          } yield assert(size)(equalTo(n))
        },
        testM("dispatches no events for disabled webhooks") {
          val n = 100

          val webhook = Webhook(
            WebhookId(0),
            "http://foo.bar",
            "testWebhook",
            WebhookStatus.Disabled,
            WebhookDeliveryMode.SingleAtMostOnce
          )

          val events = createWebhookEvent(n)(webhook.id)

          for {
            stubResponses <- Queue.unbounded[WebhookHttpResponse]
            _             <- stubResponses.offerAll(Chunk.fill(n)(WebhookHttpResponse(200)))
            _             <- TestWebhookHttpClient.setResponse(_ => Some(stubResponses))
            _             <- TestWebhookRepo.createWebhook(webhook)
            _             <- ZIO.foreach(events)(TestWebhookEventRepo.createEvent(_))
            requestsMade  <- TestWebhookHttpClient.requests
            // let test fiber sleep as we have to let requests be made to fail test
            _             <- Clock.Service.live.sleep(50.millis)
            size          <- requestsMade.takeAll.map(_.size)
          } yield assert(size)(equalTo(0))
        }
        // TODO: test that errors in the subscription crash the server?
        // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
      ) @@ timeout(5.seconds)
    ).provideSomeLayer[Has[Live.Service]](testEnv)

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
