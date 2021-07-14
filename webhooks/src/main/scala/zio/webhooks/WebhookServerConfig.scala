package zio.webhooks

import zio._
import zio.duration._

import java.time.Duration

/**
 * A [[WebhookServerConfig]] contains configuration settings for a [[WebhookServer]]'s error hub
 * capacity, retrying, and batching. For optimal performance, use capacities that are powers of 2.
 *
 * @param errorSlidingCapacity Number of errors to keep in the sliding buffer
 * @param maxSingleDispatchConcurrency Max number of single dispatches, or for retries, for each webhook
 * @param retry Configuration settings for retries
 * @param batchingCapacity Optional capacity for each batch. Set this to enable batching.
 */
final case class WebhookServerConfig(
  errorSlidingCapacity: Int,
  maxSingleDispatchConcurrency: Int,
  retry: WebhookServerConfig.Retry,
  batchingCapacity: Option[Int]
)

object WebhookServerConfig {
  val default: ULayer[Has[WebhookServerConfig]] = ZLayer.succeed(
    WebhookServerConfig(
      errorSlidingCapacity = 128,
      maxSingleDispatchConcurrency = 128,
      Retry(
        capacity = 128,
        exponentialBase = 10.millis,
        exponentialPower = 2.0,
        maxBackoff = 1.hour,
        timeout = 7.days
      ),
      batchingCapacity = None
    )
  )

  val defaultWithBatching: ULayer[Has[WebhookServerConfig]] =
    default.map(serverConfig => Has(serverConfig.get.copy(batchingCapacity = Some(128))))

  /**
   * Retry configuration settings for each webhook.
   *
   * @param capacity Max number of dispatches to hold for each webhook
   * @param exponentialBase Base duration for spacing out retries
   * @param exponentialPower Factor applied to `exponentialBase` to space out retries exponentially
   * @param timeout Max duration to wait before retries for a webhook time out
   */
  final case class Retry(
    capacity: Int,
    exponentialBase: Duration,
    exponentialPower: Double,
    maxBackoff: Duration,
    timeout: Duration
  )
}
