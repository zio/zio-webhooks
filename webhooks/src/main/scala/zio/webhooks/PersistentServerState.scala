package zio.webhooks

import zio.json._
import zio.prelude.NonEmptySet
import zio.webhooks.PersistentServerState.RetryingState

import java.time.{ Duration, Instant }

/**
 * A persistent version of [[WebhookServer.InternalState]] saved on server shutdown and loaded on
 * server restart.
 */
private[webhooks] final case class PersistentServerState(map: Map[Long, RetryingState])

private[webhooks] object PersistentServerState {

  /**
   * Persistent version of [[WebhookDispatch]].
   */
  final case class Dispatch(
    webhookId: WebhookId,
    url: String,
    deliverySemantics: WebhookDeliverySemantics,
    eventKeys: NonEmptySet[WebhookEventKey]
  )

  object Dispatch {
    def fromInternalState(dispatch: WebhookDispatch): Dispatch =
      Dispatch(
        dispatch.webhookId,
        dispatch.url,
        dispatch.deliverySemantics,
        NonEmptySet.fromNonEmptyChunk(dispatch.keys)
      )

    implicit val decoder: JsonDecoder[Dispatch] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[Dispatch] = DeriveJsonEncoder.gen
  }

  val empty: PersistentServerState = PersistentServerState(Map.empty)

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
    def fromInternalState(retry: WebhookServer.Retry): Retry =
      Retry(
        Dispatch.fromInternalState(retry.dispatch),
        backoff = retry.backoff,
        base = retry.base,
        power = retry.power,
        attempt = retry.attempt
      )

    implicit val decoder: JsonDecoder[Retry] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[Retry] = DeriveJsonEncoder.gen
  }

  /**
   * Persistent version of [[WebhookServer.WebhookState.Retrying]].
   */
  final case class RetryingState(
    sinceTime: Instant,
    retries: List[Retry]
  )

  object RetryingState {
    implicit val decoder: JsonDecoder[RetryingState] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[RetryingState] = DeriveJsonEncoder.gen
  }

  implicit val decoder: JsonDecoder[PersistentServerState] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[PersistentServerState] = DeriveJsonEncoder.gen
}
