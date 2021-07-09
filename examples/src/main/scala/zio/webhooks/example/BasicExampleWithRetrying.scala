package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.console._
import zio.duration._
import zio.magic._
import zio.stream.UStream
import zio.webhooks._
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._

/**
 * Differs from the [[BasicExample]] in that the zio-http server responds with a non-200 status half
 * the time. This behavior prompts the webhook server to retry delivery.
 */
object BasicExampleWithRetrying extends App {

  // server answers with 200 40% of the time, 404 the other
  private lazy val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      val payload = request.getBodyAsString
      for {
        n        <- random.nextIntBounded(100)
        tsString <- clock.instant.map(_.toString).map(ts => s"[$ts]: ")
        response <- ZIO
                      .foreach(payload) { payload =>
                        if (n < 40)
                          putStrLn(tsString + payload + " Response: OK") *>
                            UIO(Response.status(Status.OK))
                        else
                          putStrLn(tsString + payload + " Response: NOT_FOUND") *>
                            UIO(Response.status(Status.NOT_FOUND))
                      }
                      .orDie
      } yield response.getOrElse(Response.fromHttpError(HttpError.BadRequest("empty body")))
  }

  private lazy val n       = 500L
  private lazy val nEvents = UStream
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
      _ <- Server.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- TestWebhookRepo.createWebhook(webhook)
      _ <- nEvents.schedule(Schedule.spaced(100.micros)).foreach(TestWebhookEventRepo.createEvent)
      _ <- clock.sleep(Duration.Infinity)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookEventRepo.test,
        WebhookSttpClient.live,
        WebhookServerConfig.default,
        WebhookServer.live
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
