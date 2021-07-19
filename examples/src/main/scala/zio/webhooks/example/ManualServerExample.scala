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
 * An example of manually starting and shutting down the webhook server manually. Other than that,
 * this is the same scenario as in the [[BasicExample]].
 */
object ManualServerExample extends App {

  private val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      ZIO
        .foreach(request.getBodyAsString)(str => putStrLn(s"""SERVER RECEIVED PAYLOAD: "$str""""))
        .as(Response.status(Status.OK))
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val n = 5000

  // JSON webhook event stream
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
    .take(n.toLong)

  private lazy val port = 8080

  // Server is created and shut down manually. On shutdown, all existing work is finished before
  // the example finishes.
  private def program =
    for {
      server <- WebhookServer.create
      _      <- server.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _      <- server.start
      _      <- httpEndpointServer.start(port, httpApp).fork
      _      <- TestWebhookRepo.createWebhook(webhook)
      _      <- nEvents
                  .schedule(Schedule.fixed(1.milli))
                  .foreach(TestWebhookEventRepo.createEvent)
                  .ensuring((server.shutdown *> putStrLn("Shutdown successful")).orDie)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookEventRepo.test,
        WebhookSttpClient.live,
        WebhookServerConfig.default
      )
      .exitCode

  private lazy val webhook = Webhook(
    id = WebhookId(0),
    url = s"http://0.0.0.0:$port/endpoint",
    label = "test webhook",
    WebhookStatus.Enabled,
    WebhookDeliveryMode.SingleAtMostOnce
  )
}
