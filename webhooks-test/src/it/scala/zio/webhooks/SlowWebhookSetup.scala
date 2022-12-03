package zio.webhooks

import zio.stream.ZStream
import zio.Chunk

import WebhookServerIntegrationSpecUtil.port

final case class SlowWebhookSetup(
  webhookCount: Int,
  eventsPerWebhook: Int
) {
  val testWebhooks: Seq[Webhook] = (0 until webhookCount).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce,
      None
    )
  }

  // 100 streams with 1000 events each
  val eventStreams: ZStream[Any, Nothing, WebhookEvent] =
    ZStream.mergeAllUnbounded()(
      (0L until webhookCount.toLong).map(webhookId =>
        ZStream
          .iterate(0L)(_ + 1)
          .map { eventId =>
            WebhookEvent(
              WebhookEventKey(WebhookEventId(eventId), WebhookId(webhookId)),
              WebhookEventStatus.New,
              eventId.toString,
              Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
              None
            )
          }
          .take(eventsPerWebhook.toLong)
      ): _*
    )
}
