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
 * Differs from the [[BasicExampleWithRetrying]] in that a custom configuration is provided.
 * This also serves as an example of a scenario where deliveries are batched and are retried in
 * batches when delivery fails. A max retry backoff of 2 seconds should be seen when running this
 * example.
 */
object CustomConfigExample extends App {

  private lazy val customConfig: ULayer[Has[WebhookServerConfig]] =
    ZLayer.succeed(
      WebhookServerConfig(
        errorSlidingCapacity = 64,
        maxSingleDispatchConcurrency = 512,
        WebhookServerConfig.Retry(
          capacity = 1024,
          exponentialBase = 100.millis,
          exponentialFactor = 1.5,
          maxBackoff = 2.seconds,
          timeout = 1.day
        ),
        batchingCapacity = Some(1024)
      )
    )

  // server answers with 200 40% of the time, 404 the other
  private lazy val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      val payload = request.getBodyAsString
      for {
        n        <- random.nextIntBounded(100)
        tsString <- clock.instant.map(_.toString).map(ts => s"[$ts]: ")
        response <- ZIO
                      .foreach(payload) { payload =>
                        if (n < 20)
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

  private lazy val n       = 2000L
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
      _ <- httpEndpointServer.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- TestWebhookRepo.createWebhook(webhook)
      _ <- nEvents.schedule(Schedule.spaced(333.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
      _ <- zio.clock.sleep(Duration.Infinity)
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
