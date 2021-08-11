package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.console._
import zio.duration._
import zio.magic._
import zio.stream.UStream
import zio.webhooks.backends.InMemoryWebhookStateRepo
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.webhooks.{ WebhooksProxy, _ }

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

  // JSON webhook event stream
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

  private lazy val port = 8080

  // Server is created and shut down manually. On shutdown, all existing work is finished before
  // the example finishes.
  private def program =
    for {
      server <- WebhookServer.start
      _      <- server.subscribeToErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _      <- httpEndpointServer.start(port, httpApp).fork
      _      <- TestWebhookRepo.setWebhook(webhook)
      _      <- events
                  .schedule(Schedule.spaced(50.micros).jittered)
                  .foreach(TestWebhookEventRepo.createEvent)
                  .ensuring((server.shutdown *> putStrLn("Shutdown successful")).orDie)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
        InMemoryWebhookStateRepo.live,
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookSttpClient.live,
        WebhookServerConfig.default,
        WebhooksProxy.live
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
