package zio.webhooks.internal

import zio._
import zio.duration._
import zio.webhooks.WebhookServerConfig

import java.time.Instant

/**
 * Represents the current state of the retry logic for a [[RetryDispatcher]].
 */
private[webhooks] final case class RetryState(
  activeSinceTime: Instant,
  backoff: Option[Duration],
  failureCount: Int,
  isActive: Boolean,
  lastRetryTime: Instant,
  timeoutDuration: Duration,
  timerKillSwitch: Option[Promise[Nothing, Unit]]
) {

  /**
   * Progresses retrying to the next exponential backoff.
   */
  def increaseBackoff(timestamp: Instant, retryConfig: WebhookServerConfig.Retry): RetryState = {
    val nextExponential = retryConfig.exponentialBase * math.pow(2, failureCount.toDouble)
    val nextBackoff     = if (nextExponential >= retryConfig.maxBackoff) retryConfig.maxBackoff else nextExponential
    val nextAttempt     = if (nextExponential >= retryConfig.maxBackoff) failureCount else failureCount + 1
    copy(
      failureCount = nextAttempt,
      lastRetryTime = timestamp,
      backoff = backoff.map(_ => nextBackoff).orElse(Some(retryConfig.exponentialBase))
    )
  }

  def resetBackoff(timestamp: Instant): RetryState =
    copy(failureCount = 0, lastRetryTime = timestamp, backoff = None)

  /**
   * Kills the current timer, marking this retry inactive.
   */
  def deactivate: UIO[RetryState] =
    ZIO.foreach_(timerKillSwitch)(_.succeed(())).as(copy(isActive = false, timerKillSwitch = None))
}
