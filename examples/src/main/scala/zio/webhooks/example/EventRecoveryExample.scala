package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.console._
import zio.duration._
import zio.magic._
import zio.stream.UStream
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._
import zio.webhooks.{ WebhooksProxy, _ }

/**
 * An example of a webhook server performing event recovery on restart for a webhook with
 * at-least-once delivery semantics and a flaky endpoint. A third of `n` events are published,
 * followed by the second third of `n` events on restart, and the last third on a second restart.
 * Events that haven't been marked delivered prior to shutdown are retried on restart. All `n`
 * events are eventually delivered.
 */
object EventRecoveryExample extends App {

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
    .take(n)

  // server answers with 200 70% of the time, 404 the other
  private def httpApp(payloads: Ref[Set[String]]) =
    HttpApp.collectM {
      case request @ Method.POST -> Root / "endpoint" =>
        val payload = request.getBodyAsString
        for {
          n        <- random.nextIntBounded(100)
          tsString <- clock.instant.map(_.toString).map(ts => s"[$ts]")
          response <- ZIO
                        .foreach(payload) { payload =>
                          if (n < 70)
                            for {
                              newSize <- payloads.modify { set =>
                                           val newSet = set + payload
                                           (newSet.size, newSet)
                                         }
                              line     = s"$tsString: $payload Response: OK, events delivered: $newSize"
                              _       <- putStrLn(line)
                            } yield Response.status(Status.OK)
                          else
                            putStrLn(s"$tsString: $payload Response: NOT_FOUND") *>
                              UIO(Response.status(Status.NOT_FOUND))
                        }
                        .orDie
        } yield response.getOrElse(Response.fromHttpError(HttpError.BadRequest("empty body")))
    }

  // just an alias for a zio-http server to disambiguate it with the webhook server
  private lazy val httpEndpointServer = Server

  private lazy val n = 3000L

  private lazy val port = 8080

  private def program =
    for {
      _ <- WebhookServer.start.use { server =>
             for {
               _        <- server.subscribeToErrors
                             .use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
                             .fork
               payloads <- Ref.make(Set.empty[String])
               _        <- httpEndpointServer.start(port, httpApp(payloads)).fork
               _        <- TestWebhookRepo.setWebhook(webhook)
               _        <- events
                             .take(n / 3)
                             .schedule(Schedule.spaced(50.micros))
                             .foreach(TestWebhookEventRepo.createEvent)
             } yield ()
           }
      _ <- putStrLn("Shutdown successful")
      _ <- WebhookServer.start.use { server =>
             for {
               _ <- server.subscribeToErrors
                      .use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
                      .fork
               _ <- putStrLn("Restart successful")
               _ <- events
                      .drop(n / 3)
                      .take(n / 3)
                      .schedule(Schedule.spaced(50.micros))
                      .foreach(TestWebhookEventRepo.createEvent)
             } yield ()
           }
      _ <- putStrLn("Shutdown successful")
      _ <- WebhookServer.start.use { server =>
             for {
               _ <- server.subscribeToErrors
                      .use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
                      .fork
               _ <- putStrLn("Restart successful")
               _ <- events
                      .drop(2 * n / 3)
                      .schedule(Schedule.spaced(50.micros))
                      .foreach(TestWebhookEventRepo.createEvent)
               _ <- clock.sleep(Duration.Infinity)
             } yield ()
           }
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
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
    WebhookDeliveryMode.SingleAtLeastOnce
  )
}
