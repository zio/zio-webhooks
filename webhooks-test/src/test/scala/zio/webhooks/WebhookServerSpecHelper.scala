package zio.webhooks

import zio.Chunk

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
}
