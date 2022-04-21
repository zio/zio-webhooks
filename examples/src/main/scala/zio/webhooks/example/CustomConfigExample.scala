package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._

import zio.stream.UStream
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.{ WebhooksProxy, _ }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.{ Clock, Random, ZIOAppDefault }
import zio.Console.{ printLine, printLineError }

/**
 * Differs from the [[BasicExampleWithRetrying]] in that a custom configuration is provided.
 * This also serves as an example of a scenario where deliveries are batched and are retried in
 * batches when delivery fails. A max retry backoff of 2 seconds should be seen when running this
 * example.
 */
object CustomConfigExample extends ZIOAppDefault {

  private lazy val customConfig: ULayer[WebhookServerConfig] =
    ZLayer.succeed(
      WebhookServerConfig(
        errorSlidingCapacity = 64,
        maxRequestsInFlight = 512,
        WebhookServerConfig.Retry(
          capacity = 1024,
          exponentialBase = 100.millis,
          exponentialPower = 1.5,
          maxBackoff = 2.seconds,
          timeout = 1.day
        ),
        batchingCapacity = Some(1024),
        webhookQueueCapacity = 512
      )
    )

  private lazy val events = UStream
    .iterate(0L)(_ + 1)
    .map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i), webhook.id),
        WebhookEventStatus.New,
        s"""{"payload":$i}""",
        Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
        None
      )
    }
    .take(n)

  // server answers with 200 40% of the time, 404 the other
  private lazy val httpApp = Http.collectZIO[Request] {
    case request @ Method.POST -> !! / "endpoint" =>
      for {
        n        <- Random.nextIntBounded(100)
        tsString <- Clock.instant.map(_.toString).map(ts => s"[$ts]: ")
        response <- request.bodyAsString.flatMap { payload =>
                      if (n < 40)
                        printLine(tsString + payload + " Response: Ok") *>
                          UIO.succeed(Response.status(Status.Ok))
                      else
                        printLine(tsString + payload + " Response: NotFound") *>
                          UIO.succeed(Response.status(Status.NotFound))
                    }.orDie
      } yield response
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val n = 2000L

  private lazy val port = 8080

  private def program =
    ZIO.scoped {
      for {
        _ <- httpEndpointServer.start(port, httpApp).fork
        _ <- WebhookServer.getErrors.flatMap(UStream.fromQueue(_).map(_.toString).foreach(printLineError(_))).fork
        _ <- TestWebhookRepo.setWebhook(webhook)
        _ <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
        _ <- zio.Clock.sleep(Duration.Infinity)
      } yield ()
    }

  override def run =
    program
      .provide(
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookSttpClient.live,
        customConfig,
        WebhookServer.live,
        WebhooksProxy.live
      )
      .exitCode

  private lazy val webhook = Webhook(
    id = WebhookId(0),
    url = s"http://0.0.0.0:$port/endpoint",
    label = "test webhook",
    WebhookStatus.Enabled,
    WebhookDeliveryMode.BatchedAtLeastOnce,
    None
  )

}
