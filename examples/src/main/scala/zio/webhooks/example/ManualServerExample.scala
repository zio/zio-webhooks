package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.stream.ZStream
import zio.webhooks.backends.{InMemoryWebhookStateRepo, JsonPayloadSerialization}
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.webhooks.{WebhooksProxy, _}
import zio.ZIOAppDefault
import zio.Console.{printLine, printLineError}

/**
 * An example of manually starting a server. The server is shut down as its release action,
 * releasing its dependencies as well. Other than that, this is the same scenario as in the
 * [[BasicExample]].
 */
object ManualServerExample extends ZIOAppDefault {

  // JSON webhook event stream
  private lazy val events = ZStream
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
      request.bodyAsString
        .flatMap(str => printLine(s"""SERVER RECEIVED PAYLOAD: "$str""""))
        .as(Response.status(Status.Ok))
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val port = 8080

  // Server is created and shut down manually. On shutdown, all existing work is finished before
  // the example finishes.
  private def program =
    WebhookServer.start.flatMap { server =>
      for {
        _ <- server.subscribeToErrors.flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printLineError(_))).fork
        _ <- httpEndpointServer.start(port, httpApp).fork
        _ <- TestWebhookRepo.setWebhook(webhook)
        _ <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
      } yield ()
    } *> printLine("Shutdown successful").orDie

  override def run =
    program
      .provideSome[Scope](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
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
    WebhookDeliveryMode.SingleAtMostOnce,
    None
  )
}
