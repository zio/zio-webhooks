package zio.webhooks

import zio._
import zio.webhooks.testkit._

// TODO: scaladoc
object WebhookServerSpecHelper {

  def createWebhooks(n: Int)(status: WebhookStatus, deliveryMode: WebhookDeliveryMode): Iterable[Webhook] =
    (0 until n).map { i =>
      Webhook(
        WebhookId(i.toLong),
        "http://example.org/" + i,
        "testWebhook" + i,
        status,
        deliveryMode
      )
    }

  def createWebhookEvent(n: Int)(webhookId: WebhookId): Iterable[WebhookEvent] =
    (0 until n).map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i.toLong), webhookId),
        WebhookEventStatus.New,
        "lorem ipsum " + i,
        Chunk(("Accept", "*/*"))
      )
    }

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
