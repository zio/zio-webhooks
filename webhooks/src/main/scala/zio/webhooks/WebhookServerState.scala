package zio.webhooks

import zio.json._
import zio.webhooks.WebhookServerState.RetryingState

import java.time.{ Duration, Instant }

/**
 * A persistent version of [[WebhookServer.InternalState]] saved on server shutdown and loaded on
 * server restart.
 */
private[webhooks] final case class WebhookServerState(map: Map[Long, RetryingState])

private[webhooks] object WebhookServerState {

  /**
   * Persistent version of [[WebhookDispatch]].
   */
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

  /**
   * Persistent version of [[WebhookServer.Retry]].
   */
  final case class Retry(
    dispatch: Dispatch,
    backoff: Option[Duration],
    base: Duration,
    power: Double,
    attempt: Int
  )

  object Retry {
    implicit val decoder: JsonDecoder[Retry] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[Retry] = DeriveJsonEncoder.gen
  }

  /**
   * Persistent version of [[WebhookServer.WebhookState.Retrying]].
   */
  final case class RetryingState(
    sinceTime: Instant,
    dispatches: List[Retry]
  )

  object RetryingState {
    implicit val decoder: JsonDecoder[RetryingState] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[RetryingState] = DeriveJsonEncoder.gen
  }

  implicit val decoder: JsonDecoder[WebhookServerState] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[WebhookServerState] = DeriveJsonEncoder.gen
}
