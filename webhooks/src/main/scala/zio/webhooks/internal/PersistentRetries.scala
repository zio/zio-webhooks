package zio.webhooks.internal

import zio.json._
import zio.webhooks.internal.PersistentRetries.RetryState

import java.time.{ Duration, Instant }

/**
 * A persistent version of [[zio.webhooks.WebhookServer.Retries]] saved on server shutdown and loaded on server
 * restart.
 */
private[webhooks] final case class PersistentRetries(retryStates: Map[Long, RetryState])

private[webhooks] object PersistentRetries {
  val empty: PersistentRetries = PersistentRetries(Map.empty)

  /**
   * Persistent version of [[zio.webhooks.WebhookServer.RetryState]].
   */
  final case class RetryState(
    activeSinceTime: Instant,
    backoff: Option[Duration],
    failureCount: Int,
    lastRetryTime: Instant,
    timeLeft: Duration
  )

  object RetryState {
    implicit val decoder: JsonDecoder[RetryState] = DeriveJsonDecoder.gen
    implicit val encoder: JsonEncoder[RetryState] = DeriveJsonEncoder.gen
  }

  implicit val decoder: JsonDecoder[PersistentRetries] = DeriveJsonDecoder.gen
  implicit val encoder: JsonEncoder[PersistentRetries] = DeriveJsonEncoder.gen
}
