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
 * Differs from the [[BasicExample]] in that a custom configuration is provided. This also serves as
 * an example of a scenario where dispatches are batched and are retried when delivery fails.
 */
object CustomConfigExample extends App {

  private lazy val customConfig: ULayer[Has[WebhookServerConfig]] =
    ZLayer.succeed(
      WebhookServerConfig(
        errorSlidingCapacity = 64,
        WebhookServerConfig.Retry(
          capacity = 64,
          exponentialBase = 1.second,
          exponentialFactor = 1.5,
          timeout = 1.day
        ),
        Some(256)
      )
    )

  private lazy val events = UStream.iterate(0L)(_ + 1).map { i =>
    WebhookEvent(
      WebhookEventKey(WebhookEventId(i), webhook.id),
      WebhookEventStatus.New,
      s"""{"payload":$i}""",
      Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
    )
  }

  // server answers with 200 10% of the time, 404 the other
  private lazy val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      for {
        _        <- clock.instant.map(_.toString).flatMap(ts => putStr(s"[$ts]: "))
        _        <- ZIO.foreach_(request.getBodyAsString)(str => putStrLn(s"""SERVER RECEIVED PAYLOAD: "$str""""))
        n        <- random.nextIntBounded(100)
        _        <- clock.instant.map(_.toString).flatMap(ts => putStr(s"[$ts]: "))
        response <- if (n < 10)
                      putStrLn("Server responding with OK") *> UIO(Response.status(Status.OK))
                    else
                      putStrLn("Server responding with NOT_FOUND") *> UIO(Response.status(Status.NOT_FOUND))
      } yield response
  }

  private lazy val port = 8080

  private def program =
    for {
      _ <- Server.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- TestWebhookRepo.createWebhook(webhook)
      _ <- events.schedule(Schedule.fixed(200.millis)).foreach(TestWebhookEventRepo.createEvent)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookEventRepo.test,
        WebhookSttpClient.live,
        customConfig,
        WebhookServer.live
      )
      .exitCode

  private lazy val webhook = Webhook(
    id = WebhookId(0),
    url = s"http://0.0.0.0:$port/endpoint",
    label = "test webhook",
    WebhookStatus.Enabled,
    WebhookDeliveryMode.BatchedAtLeastOnce
  )
}
