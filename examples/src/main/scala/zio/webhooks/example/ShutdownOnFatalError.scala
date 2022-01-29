package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._

import zio.stream._
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.webhooks.{ WebhooksProxy, _ }
import zio.ZIOAppDefault
import zio.Console.{ printLine, printLineError }

/**
 * An example of how the server shuts down when encountering a fatal error: in this case a missing
 * webhook.
 */
object ShutdownOnFatalError extends ZIOAppDefault {

  private lazy val events = goodEvents.take(2) ++ UStream(eventWithoutWebhook) ++ goodEvents.drop(2)

  private lazy val eventWithoutWebhook = WebhookEvent(
    WebhookEventKey(WebhookEventId(-1), WebhookId(-1)),
    WebhookEventStatus.New,
    s"""{"payload":-1}""",
    Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
    None
  )

  private val goodEvents = UStream
    .iterate(0L)(_ + 1)
    .map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i), webhook.id),
        WebhookEventStatus.New,
        s"""{"payload":$i}""",
        Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
        None
      )
    }

  private val httpApp = Http.collectZIO[Request] {
    case request @ Method.POST -> !! / "endpoint" =>
      request.getBodyAsString
        .flatMap(str => printLine(s"""SERVER RECEIVED PAYLOAD: "$str""""))
        .as(Response.status(Status.OK))
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val port = 8080

  private def program =
    for {
      errorFiber <- WebhookServer.getErrors.use(UStream.fromQueue(_).runHead).fork
      _          <- TestWebhookRepo.setWebhook(webhook)
      eventFiber <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent).fork
      httpFiber  <- httpEndpointServer.start(port, httpApp).fork
      _          <- errorFiber.join
                      .flatMap(error => printLineError(error.toString))
                      .onExit(_ => (eventFiber zip httpFiber).interrupt)
    } yield ()

  override def run: ZIO[ZEnv with ZIOAppArgs, Any, Any] =
    program
      .provideCustom(
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookRepo.test,
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
    WebhookDeliveryMode.SingleAtLeastOnce,
    None
  )
}
