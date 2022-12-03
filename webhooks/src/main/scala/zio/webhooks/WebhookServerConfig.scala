package zio.webhooks

import zio._

import java.time.Duration

/**
 * A [[WebhookServerConfig]] contains configuration settings for a [[WebhookServer]]'s error hub
 * capacity, retrying, and batching. For optimal performance, use capacities that are powers of 2.
 *
 * @param errorSlidingCapacity Number of errors to keep in the sliding buffer
 * @param maxRequestsInFlight Max number of requests allowed at any given time
 * @param retry Configuration settings for retries
 * @param batchingCapacity Optional capacity for each batch. Set this to enable batching.
 */
final case class WebhookServerConfig(
  errorSlidingCapacity: Int,
  maxRequestsInFlight: Int,
  retry: WebhookServerConfig.Retry,
  batchingCapacity: Option[Int],
  webhookQueueCapacity: Int
)

object WebhookServerConfig {
  val default: ULayer[WebhookServerConfig] = ZLayer.succeed(
    WebhookServerConfig(
      errorSlidingCapacity = 128,
      maxRequestsInFlight = 256,
      Retry(
        capacity = 128,
        exponentialBase = 10.millis,
        exponentialPower = 2.0,
        maxBackoff = 1.hour,
        timeout = 7.days
      ),
      batchingCapacity = None,
      webhookQueueCapacity = 256
    )
  )

  val defaultWithBatching: ULayer[WebhookServerConfig] =
    default.update(serverConfig => serverConfig.copy(batchingCapacity = Some(128)))

  /**
   * Retry configuration settings for each webhook.
   *
   * @param capacity Max number of dispatches to hold for each webhook
   * @param exponentialBase Base duration for retry backoff
   * @param exponentialPower Factor repeatedly applied to `exponentialBase` for exponential retry backoff
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
