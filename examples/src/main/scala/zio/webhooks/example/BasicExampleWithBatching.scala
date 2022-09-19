package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.stream.ZStream
import zio.webhooks.backends.{InMemoryWebhookStateRepo, JsonPayloadSerialization}
import zio.webhooks.{WebhooksProxy, _}
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.{Random, ZIOAppDefault}
import zio.Console.{printLine, printLineError}

/**
 * Differs from the [[BasicExample]] in that events are batched with the default batching setting
 * of 128 elements per batch. The server dispatches all events queued up for each webhook since the
 * last delivery and sends them in a batch.
 */
object BasicExampleWithBatching extends ZIOAppDefault {

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
      for {
        randomDelay <- Random.nextIntBetween(10, 20).map(_.millis)
        response    <- request.body.asString.flatMap { str =>
                         printLine(s"""SERVER RECEIVED PAYLOAD: "$str"""")
                       }
                         .as(Response.status(Status.Ok))
                         .delay(randomDelay)
      } yield response
  }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val port = 8080

  private def program =
    for {
      _ <- httpEndpointServer.start(port, httpApp).fork
      _ <- WebhookServer.getErrors.flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printLineError(_))).fork
      _ <- TestWebhookRepo.setWebhook(webhook)
      _ <- events.schedule(Schedule.spaced(50.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
    } yield ()

  override def run =
    program
      .provideSome[Scope](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookSttpClient.live,
        WebhookServerConfig.defaultWithBatching,
        WebhookServer.live,
        WebhooksProxy.live
      )
      .exitCode

  // Delivery mode is set to Batched
  private lazy val webhook = Webhook(
    id = WebhookId(0),
    url = s"http://0.0.0.0:$port/endpoint",
    label = "test webhook",
    WebhookStatus.Enabled,
    WebhookDeliveryMode.BatchedAtMostOnce,
    None
  )
}
