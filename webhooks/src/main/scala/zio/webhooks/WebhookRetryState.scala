package zio.webhooks

import zio.json._
import zio.webhooks.WebhookRetryState.Dispatch

import java.time.Instant

/**
 * A codec-friendly version of [[WebhookServer.WebhookState.Retrying]].
 */
private[webhooks] final case class WebhookRetryState(
  sinceTime: Instant,
  dispatches: List[Dispatch]
)

private[webhooks] object WebhookRetryState {
  final case class Dispatch(
    webhookId: WebhookId,
    url: String,
    deliverySemantics: WebhookDeliverySemantics,
    eventKeys: List[WebhookEventKey]
  )

  object Dispatch {
    implicit val decoder: JsonDecoder[Dispatch] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[Dispatch] = DeriveJsonEncoder.gen
  }

  implicit val decoder: JsonDecoder[WebhookRetryState] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[WebhookRetryState] = DeriveJsonEncoder.gen
}
