package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.console._
import zio.duration._
import zio.magic._
import zio.stream._
import zio.webhooks.WebhookError._
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.webhooks.{ WebhooksProxy, _ }

/**
 * An example of how to shut down the server on the first error encountered.
 */
object ShutdownOnFirstError extends App {

  private val goodEvents = UStream
    .iterate(0L)(_ + 1)
    .map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i), webhook.id),
        WebhookEventStatus.New,
        s"""{"payload":$i}""",
        Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
      )
    }

  private lazy val events = goodEvents.take(2) ++ UStream(eventWithoutWebhook) ++ goodEvents.drop(2)

  private lazy val eventWithoutWebhook = WebhookEvent(
    WebhookEventKey(WebhookEventId(-1), WebhookId(-1)),
    WebhookEventStatus.New,
    s"""{"payload":-1}""",
    Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
  )

  private val httpApp = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" =>
      ZIO
        .foreach(request.getBodyAsString)(str => putStrLn(s"""SERVER RECEIVED PAYLOAD: "$str""""))
        .as(Response.status(Status.OK))
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val port = 8080

  private def program = {
    for {
      errorFiber <- WebhookServer.getErrors.use(_.take.flip).fork
      httpFiber  <- httpEndpointServer.start(port, httpApp).fork
      _          <- TestWebhookRepo.setWebhook(webhook)
      _          <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent).fork
      _          <- errorFiber.join.onExit(_ => WebhookServer.shutdown.orDie *> httpFiber.interrupt)
    } yield ()
  }.catchAll {
    case BadWebhookUrlError(url, message) => putStrLnErr(s"""Bad url "$url", reason: $message """)
    case InvalidStateError(_, message)    => putStrLnErr(s"Invalid state: $message")
    case MissingWebhookError(id)          => putStrLnErr(s"Missing webhook: $id")
    case MissingEventError(key)           => putStrLnErr(s"Missing event: $key")
    case MissingEventsError(keys)         => putStrLnErr(s"Missing events: $keys")
  }

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
        TestWebhookEventRepo.test,
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookServer.live,
        WebhookServerConfig.default,
        WebhookSttpClient.live,
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
