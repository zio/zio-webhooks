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

object ManualServerExample extends App {

  private lazy val events = UStream.iterate(0L)(_ + 1).map { i =>
    WebhookEvent(
      WebhookEventKey(WebhookEventId(i), webhook.id),
      WebhookEventStatus.New,
      s"""{"payload":$i}""",
      Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
    )
  }

  private val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      ZIO
        .foreach(request.getBodyAsString)(str => putStrLn(s"""SERVER RECEIVED PAYLOAD: "$str""""))
        .as(Response.status(Status.OK))
  }

  private lazy val port = 8080

  private def program =
    for {
      server <- WebhookServer.create
      _      <- server.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _      <- server.start
      _      <- Server.start(port, httpApp).fork
      _      <- TestWebhookRepo.createWebhook(webhook)
      _      <- events.schedule(Schedule.fixed(1.second)).foreach(TestWebhookEventRepo.createEvent)
      _      <- server.shutdown
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
    WebhookDeliveryMode.SingleAtLeastOnce
  )
}
