package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.stream.ZStream
import zio.webhooks.backends.{InMemoryWebhookStateRepo, JsonPayloadSerialization}
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.webhooks.{WebhooksProxy, _}
import zio.{Random, ZIOAppDefault}
import zio.Console.{printLine, printLineError}

/**
 * Runs a webhook server and a zio-http server to which webhook events are delivered. The webhook
 * server is started as part of layer construction, and shut down when the example app is closed.
 *
 * Errors are printed to the console's error channel. A webhook and a stream of events are created
 * and the events are delivered to an endpoint one-by-one. The zio-http endpoint prints out the
 * contents of each payload as it receives them.
 */
object BasicExample extends ZIOAppDefault {

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

  // reliable endpoint
  private val httpApp = Http.collectZIO[Request] {
    case request @ Method.POST -> !! / "endpoint" =>
      for {
        randomDelay <- Random.nextIntBounded(300).map(_.millis)
        response    <- request.bodyAsString
                         .flatMap(str => printLine(s"""SERVER RECEIVED PAYLOAD: "$str""""))
                         .as(Response.status(Status.Ok))
                         .delay(randomDelay) // random delay to simulate latency
      } yield response
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val port = 8080

  private def program =
    for {
      _ <- httpEndpointServer.start(port, httpApp).forkScoped
      _ <- WebhookServer.getErrors.flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printLineError(_))).forkScoped
      _ <- TestWebhookRepo.setWebhook(webhook)
      _ <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
    } yield ()

  /**
   * The webhook server is started as part of the layer construction. See `WebhookServer.live`.
   */
  override def run =
    program
      .provideSome[Scope](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookSttpClient.live,
        WebhookServerConfig.default,
        WebhookServer.live,
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
