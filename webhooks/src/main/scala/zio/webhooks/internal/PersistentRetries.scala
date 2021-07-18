package zio.webhooks.internal

import zio.json._
import zio.webhooks.internal.PersistentRetries.RetryingState

import java.time.{ Duration, Instant }

/**
 * A persistent version of [[WebhookServer.Retries]] saved on server shutdown and loaded on server
 * restart.
 */
private[webhooks] final case class PersistentRetries(retryStates: Map[Long, RetryingState])

private[webhooks] object PersistentRetries {
  val empty: PersistentRetries = PersistentRetries(Map.empty)

  /**
   * Persistent version of [[zio.webhooks.WebhookServer.RetryState]].
   */
  final case class RetryingState(
    activeSinceTime: Instant,
    backoff: Duration,
    failureCount: Int,
    lastRetryTime: Instant,
    timeLeft: Duration
  )

  object RetryingState {
    implicit val decoder: JsonDecoder[RetryingState] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[RetryingState] = DeriveJsonEncoder.gen
  }

  implicit val decoder: JsonDecoder[PersistentRetries] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[PersistentRetries] = DeriveJsonEncoder.gen
}
