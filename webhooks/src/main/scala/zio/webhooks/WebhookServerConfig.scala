package zio.webhooks

import zio._
import zio.duration._
import zio.webhooks.WebhookServerConfig.Batching

import java.time.Duration

case class WebhookServerConfig(
  errorSlidingCapacity: Int,
  retry: WebhookServerConfig.Retry,
  batching: Option[Batching] = None
)

// TODO: pull in zio-config, add this in solution
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

  final case class Batching(queue: Queue[(Webhook, WebhookEvent)], maxSize: Int, maxWaitTime: Duration)

  object Batching {
    val default: ULayer[Has[Batching]] = make(128)

    def make(capacity: Int): ULayer[Has[Batching]] =
      Queue
        .bounded[(Webhook, WebhookEvent)](capacity)
        .map(queue => Batching(queue, maxSize = 10, maxWaitTime = 5.seconds))
        .toLayer
  }

  final case class Retry(
    capacity: Int,
    exponentialBase: Duration,
    exponentialFactor: Double,
    timeout: Duration
  )
}
