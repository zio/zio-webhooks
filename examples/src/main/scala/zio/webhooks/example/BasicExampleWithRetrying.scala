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
 * Differs from the [[BasicExample]] in that the zio-http server responds with a non-200 status some
 * of the time. This behavior prompts the webhook server to retry delivery. The server will keep on
 * retrying events for a webhook with at-least-once delivery semantics one-by-one until the server
 * successfully marks all `n` events delivered.
 */
object BasicExampleWithRetrying extends ZIOAppDefault {

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

  // a flaky server answers with 200 60% of the time, 404 the other
  private lazy val httpApp = Http.collectZIO[Request] {
    case request @ Method.POST -> !! / "endpoint" =>
      for {
        n        <- Random.nextIntBounded(100)
        tsString <- Clock.instant.map(_.toString).map(ts => s"[$ts]: ")
        response <- request.bodyAsString.flatMap { payload =>
                      if (n < 60)
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
        _ <- printLine("starting http")
        _ <- httpEndpointServer.start(port, httpApp).forkScoped
        _ <- WebhookServer.getErrors.flatMap(UStream.fromQueue(_).map(_.toString).foreach(printLineError(_))).forkScoped
        _ <- printLine("set webhook")
        _ <- TestWebhookRepo.setWebhook(webhook)
        _ <- printLine("scheduling events")
        _ <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
        _ <- Clock.sleep(Duration.Infinity)
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
        WebhookServerConfig.default,
        WebhookServer.live,
        WebhooksProxy.live
      )
      .exitCode

  private lazy val webhook = Webhook(
    id = WebhookId(0),
    url = s"http://0.0.0.0:$port/endpoint",
    label = "test webhook",
    WebhookStatus.Enabled,
    WebhookDeliveryMode.SingleAtLeastOnce,
    None
  )
}
