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
 * Runs a webhook server and a zio-http server to which webhook events are delivered. The webhook
 * server is started as part of layer construction, and shut down when the example app is closed.
 *
 * Errors are printed to the console's error channel. A webhook and a stream of events are created
 * and the events are delivered to an endpoint one-by-one. The zio-http endpoint prints out the
 * contents of each payload as it receives them.
 */
object BasicExample extends App {

  // reliable endpoint
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

  private def program =
    for {
      _ <- httpEndpointServer.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- TestWebhookRepo.setWebhook(webhook)
      _ <- nEvents.schedule(Schedule.fixed(10.milli)).foreach(TestWebhookEventRepo.createEvent)
    } yield ()

  /**
   * The webhook server is started as part of the layer construction. See [[WebhookServer.live]].
   */
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
    WebhookDeliveryMode.SingleAtMostOnce
  )
}
