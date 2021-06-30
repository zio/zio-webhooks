package zio.webhooks

import zio._
import zio.duration._

import java.time.Duration

/**
 * A [[WebhookServerConfig]] contains configuration settings for a [[WebhookServer]]'s error hub
 * capacity, retrying, and batching. For optimal performance, use capacities that are powers of 2.
 */
final case class WebhookServerConfig(
  errorSlidingCapacity: Int,
  retry: WebhookServerConfig.Retry,
  enableBatching: Boolean = false
)

object WebhookServerConfig {
  val default: ULayer[Has[WebhookServerConfig]] = ZLayer.succeed(
    WebhookServerConfig(
      errorSlidingCapacity = 128,
      Retry(
        capacity = 128,
        exponentialBase = 10.millis,
        exponentialFactor = 2.0,
        timeout = 7.days
      )
    )
  )

  val defaultWithBatching: ULayer[Has[WebhookServerConfig]] =
    default.map(serverConfig => Has(serverConfig.get.copy(enableBatching = true)))

  /**
   * Retry configuration settings
   *
   * @param capacity Max number of dispatches to hold for each webhook
   * @param exponentialBase Base duration for spacing out retries
   * @param exponentialFactor Factor applied to `exponentialBase` to space out retries exponentially
   * @param timeout Max duration to wait before retries for a webhook time out
   */
  final case class Retry(
    capacity: Int,
    exponentialBase: Duration,
    exponentialFactor: Double,
    timeout: Duration
  )
}
