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

  private lazy val events = UStream.iterate(0L)(_ + 1).map { i =>
    WebhookEvent(
      WebhookEventKey(WebhookEventId(i), webhook.id),
      WebhookEventStatus.New,
      s"""{"payload":$i}""",
      Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
    )
  }

  // server answers with 200 half the time, 404 the other
  private val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      ZIO.foreach_(request.getBodyAsString)(str => putStrLn(s"""SERVER RECEIVED PAYLOAD: "$str"""")) *>
        random.nextBoolean.map(if (_) Response.status(Status.OK) else Response.status(Status.NOT_FOUND))
  }

  private lazy val port = 8080

  private def program =
    for {
      _ <- Server.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- TestWebhookRepo.createWebhook(webhook)
      _ <- events.schedule(Schedule.fixed(1.second)).foreach(TestWebhookEventRepo.createEvent)
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
