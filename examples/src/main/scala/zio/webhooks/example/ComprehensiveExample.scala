package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.stream.{ UStream, ZStream }
import zio.webhooks._
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.example.RestartingWebhookServer.testWebhooks
import zio.webhooks.testkit._

import java.io.IOException
import zio.{ Clock, Random, ZIOAppDefault }
import zio.Console.{ printLine, printLineError }

/**
 * Runs an example that simulates a comprehensive suite of scenarios that may occur during the
 * operation of a webhook server.
 */
object ComprehensiveExample extends ZIOAppDefault {

  def events: UStream[WebhookEvent] =
    ZStream
      .iterate(0L)(_ + 1)
      .zip(ZStream.repeatZIO(Random.nextIntBetween(0, 1000)))
      .map {
        case (i, webhookId) =>
          WebhookEvent(
            WebhookEventKey(WebhookEventId(i), WebhookId(webhookId.toLong)),
            WebhookEventStatus.New,
            i.toString, // a single number string is valid JSON
            Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
            None
          )
      }

  private def program =
    for {
      _ <- ZIO.foreachDiscard(testWebhooks)(TestWebhookRepo.setWebhook)
      _ <- RestartingWebhookServer.start.fork
      _ <- RandomEndpointBehavior.run.fork
      _ <- events.schedule(Schedule.spaced(25.micros).jittered).foreach(TestWebhookEventRepo.createEvent)
      _ <- Clock.sleep(Duration.Infinity)
    } yield ()

  override def run =
    program
      .provide(
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookSttpClient.live,
        WebhookServerConfig.default,
        WebhooksProxy.live
      )
      .exitCode
}

sealed trait RandomEndpointBehavior extends Product with Serializable { self =>
  import RandomEndpointBehavior._

  def start: ZIO[Any, Throwable, Any] =
    self match {
      case RandomEndpointBehavior.Down   =>
        ZIO.unit
      case RandomEndpointBehavior.Flaky  =>
        httpEndpointServer.start(port, flakyBehavior)
      case RandomEndpointBehavior.Normal =>
        httpEndpointServer.start(port, normalBehavior)
    }
}

object RandomEndpointBehavior {
  case object Down   extends RandomEndpointBehavior
  case object Flaky  extends RandomEndpointBehavior
  case object Normal extends RandomEndpointBehavior

  val flakyBehavior: UHttpApp = Http.collectZIO[Request] {
    case request @ Method.POST -> !! / "endpoint" / id =>
      val response =
        for {
          n           <- Random.nextIntBounded(100)
          timeString  <- Clock.instant.map(_.toString).map(ts => s"[$ts]: ")
          randomDelay <- Random.nextIntBounded(200).map(_.millis)
          response    <- request.body.asString.flatMap { payload =>
                           val line = s"$timeString webhook $id $payload"
                           if (n < 60)
                             printLine(line + " Response: Ok") *> ZIO.succeed(Response.status(Status.Ok))
                           else
                             printLine(line + " Response: NotFound") *> ZIO.succeed(Response.status(Status.NotFound))
                         }.orDie
                           .delay(randomDelay)
        } yield response
      response.uninterruptible
  }

  // just an alias for a zio-http server to tell it apart from the webhook server
  lazy val httpEndpointServer: Server.type = Server

  val normalBehavior = Http.collectZIO[Request] {
    case request @ Method.POST -> !! / "endpoint" / id =>
      val response =
        for {
          randomDelay <- Random.nextIntBounded(200).map(_.millis)
          response    <- request.body.asString.flatMap { str =>
                           printLine(s"""SERVER RECEIVED PAYLOAD: webhook: $id $str Ok""")
                         }
                           .as(Response.status(Status.Ok))
                           .orDie
                           .delay(randomDelay)
        } yield response
      response.uninterruptible
  }

  private lazy val port = 8080

  val randomBehavior: UIO[RandomEndpointBehavior] =
    Random.nextIntBounded(3).map {
      case 0 => Normal
      case 1 => Flaky
      case _ => Down
    }

  def run: ZIO[Any, IOException, Unit] =
    ZStream.repeatZIO(randomBehavior).foreach { behavior =>
      for {
        _ <- printLine(s"Endpoint server behavior: $behavior")
        f <- behavior.start.fork.delay(2.seconds)
        _ <- f.interrupt.delay(1.minute)
      } yield ()
    }
}

object RestartingWebhookServer {

  private lazy val port = 8080

  def start =
    runServerThenShutdown.forever

  private def runServerThenShutdown =
    for {
      _ <- printLine("Server starting")
      _ <- ZIO.scoped {
             WebhookServer.start.flatMap { server =>
               for {
                 _        <- printLine("Server started")
                 f        <- server.subscribeToErrors
                               .flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printLineError(_)))
                               .fork
                 _        <- TestWebhookEventRepo.enqueueNew
                 duration <- Random.nextIntBetween(3000, 5000).map(_.millis)
                 _        <- f.interrupt.delay(duration)
               } yield ()
             }
           }
      _ <- printLine("Server shut down")
    } yield ()

  lazy val testWebhooks = (0 until 250).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtLeastOnce,
      None
    )
  } ++ (250 until 500).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce,
      None
    )
  } ++ (500 until 750).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtLeastOnce,
      None
    )
  } ++ (750 until 1000).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtMostOnce,
      None
    )
  }
}
