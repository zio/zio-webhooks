package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.console._
import zio.duration._
import zio.magic._
import zio.stream.UStream
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.{ WebhooksProxy, _ }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._

/**
 * Differs from the [[BasicExample]] in that the zio-http server responds with a non-200 status some
 * of the time. This behavior prompts the webhook server to retry delivery. The server will keep on
 * retrying events for a webhook with at-least-once delivery semantics one-by-one until the server
 * successfully marks all `n` events delivered.
 */
object BasicExampleWithRetrying extends App {

  // a flaky server answers with 200 60% of the time, 404 the other
  private lazy val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      val payload = request.getBodyAsString
      for {
        n        <- random.nextIntBounded(100)
        tsString <- clock.instant.map(_.toString).map(ts => s"[$ts]: ")
        response <- ZIO
                      .foreach(payload) { payload =>
                        if (n < 60)
                          putStrLn(tsString + payload + " Response: OK") *>
                            UIO(Response.status(Status.OK))
                        else
                          putStrLn(tsString + payload + " Response: NOT_FOUND") *>
                            UIO(Response.status(Status.NOT_FOUND))
                      }
                      .orDie
      } yield response.getOrElse(Response.fromHttpError(HttpError.BadRequest("empty body")))
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val n      = 2000L
  private lazy val events = UStream
    .iterate(0L)(_ + 1)
    .map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i), webhook.id),
        WebhookEventStatus.New,
        s"""{"payload":$i}""",
        Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
      )
    }
    .take(n)

  private lazy val port = 8080

  private def program =
    for {
      _ <- httpEndpointServer.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- TestWebhookRepo.setWebhook(webhook)
      _ <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
      _ <- clock.sleep(Duration.Infinity)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
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
    WebhookDeliveryMode.SingleAtLeastOnce
  )
}
