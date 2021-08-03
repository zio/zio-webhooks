package zio.webhooks.internal

import zio.json._
import zio.webhooks.internal.PersistentRetries.PersistentRetryState

import java.time.{ Duration, Instant }

/**
 * A persistent version of the [[zio.webhooks.internal.RetryState]] saved on server shutdown and
 * loaded on server restart.
 */
private[webhooks] final case class PersistentRetries(retryStates: Map[Long, PersistentRetryState])

private[webhooks] object PersistentRetries {
  val empty: PersistentRetries = PersistentRetries(Map.empty)

  /**
   * Persistent version of [[zio.webhooks.internal.RetryState]].
   */
  final case class PersistentRetryState(
    activeSinceTime: Instant,
    backoff: Option[Duration],
    failureCount: Int,
    lastRetryTime: Instant,
    timeLeft: Duration
  )

  object PersistentRetryState {
    implicit val decoder: JsonDecoder[PersistentRetryState] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[PersistentRetryState] = DeriveJsonEncoder.gen
  }

  implicit val decoder: JsonDecoder[PersistentRetries] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[PersistentRetries] = DeriveJsonEncoder.gen
}
