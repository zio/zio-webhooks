package zio.webhooks

import zio._
import zio.duration._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live
import zio.webhooks.WebhookServerSpecUtil._

import java.time.Instant

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      suite("on server new event subscription")(
        testM("can dispatch single event to n webhooks") {
          val n        = 100
          val webhooks = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

          assertRequestsMade(
            stubResponses = Chunk.fill(n)(WebhookHttpResponse(200)),
            webhooks = webhooks,
            events = webhooks.map(_.id).flatMap(createWebhookEvents(1)),
            requestsAssertion = _.takeN(n).map(assert(_)(hasSize(equalTo(n))))
          )
        },
        testM("no events dispatched for disabled webhooks") {
          val n       = 100
          val webhook = Webhook(
            WebhookId(0),
            "http://foo.bar",
            "testWebhook",
            WebhookStatus.Disabled,
            WebhookDeliveryMode.SingleAtMostOnce
          )

          assertRequestsMade(
            stubResponses = Chunk.fill(n)(WebhookHttpResponse(200)),
            webhooks = List(webhook),
            events = createWebhookEvents(n)(webhook.id),
            requestsAssertion = _.takeAll.map(_.size).map(assert(_)(equalTo(0))),
            sleepDuration = Some(100.millis)
          )
        },
        testM("dispatches no events for unavailable webhooks") {
          val n       = 100
          val webhook = Webhook(
            WebhookId(0),
            "http://foo.bar",
            "testWebhook",
            WebhookStatus.Unavailable(Instant.EPOCH),
            WebhookDeliveryMode.SingleAtMostOnce
          )

          assertRequestsMade(
            stubResponses = Chunk.fill(n)(WebhookHttpResponse(200)),
            webhooks = List(webhook),
            events = createWebhookEvents(n)(webhook.id),
            requestsAssertion = _.takeAll.map(_.size).map(assert(_)(equalTo(0))),
            sleepDuration = Some(100.millis)
          )
        }
        // TODO: test that errors in the subscription crash the server?
        // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
      ) @@ timeout(5.seconds)
    ).provideSomeLayer[Has[Live.Service] with Has[Annotations.Service]](testEnv)
}
