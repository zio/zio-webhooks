package zio.webhooks

import zio._
import zio.duration._
import zio.webhooks.WebhookServerConfig.Batching

import java.time.Duration

/**
 * A [[WebhookServerConfig]] contains configuration settings for a [[WebhookServer]]'s error hub
 * capacity, retrying, and batching. For optimal performance, use capacities that are powers of 2.
 */
case class WebhookServerConfig(
  errorSlidingCapacity: Int,
  retry: WebhookServerConfig.Retry,
  batching: Option[Batching] = None
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

  val defaultWithBatching: ULayer[Has[WebhookServerConfig]] = default.zipPar(Batching.default).map {
    case (serverConfig, batching) =>
      Has(serverConfig.get.copy(batching = Some(batching.get)))
  }

  /**
   * Batching configuration settings
   *
   * @param capacity Max number of elements to hold in batching
   * @param maxSize Max number of events that should be in a batch
   * @param maxWaitTime Max amount of time to wait before a batch is made
   */
  final case class Batching(capacity: Int, maxSize: Int, maxWaitTime: Duration)

  object Batching {
    val default: ULayer[Has[Batching]] = ZLayer.succeed(Batching(128, 10, 5.seconds))
  }

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
