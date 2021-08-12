package zio.webhooks.example

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.console._
import zio.duration._
import zio.magic._
import zio.random.Random
import zio.stream.UStream
import zio.webhooks._
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.example.RestartingWebhookServer.testWebhooks
import zio.webhooks.testkit._

import java.io.IOException

/**
 * Runs an example that simulates a comprehensive suite of scenarios that may occur during the
 * operation of a webhook server.
 */
object ComprehensiveExample extends App {

  private def program =
    for {
      _ <- ZIO.foreach_(testWebhooks)(TestWebhookRepo.setWebhook)
      _ <- RestartingWebhookServer.start.fork
      _ <- RandomEndpointBehavior.run.fork
      _ <- clock.sleep(Duration.Infinity)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
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

  def start: ZIO[ZEnv, Throwable, Any] =
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

  val flakyBehavior = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" / id =>
      val payload  = request.getBodyAsString
      val response =
        for {
          n           <- random.nextIntBounded(100)
          timeString  <- clock.instant.map(_.toString).map(ts => s"[$ts]: ")
          randomDelay <- random.nextIntBounded(200).map(_.millis)
          response    <- ZIO
                           .foreach(payload) { payload =>
                             val line = s"$timeString webhook $id $payload"
                             if (n < 60)
                               putStrLn(line + " Response: OK") *> UIO(Response.status(Status.OK))
                             else
                               putStrLn(line + " Response: NOT_FOUND") *> UIO(Response.status(Status.NOT_FOUND))
                           }
                           .orDie
                           .delay(randomDelay)
        } yield response.getOrElse(Response.fromHttpError(HttpError.BadRequest("empty body")))
      response.uninterruptible
  }

  // just an alias for a zio-http server to tell it apart from the webhook server
  lazy val httpEndpointServer: Server.type = Server

  val normalBehavior = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" / id =>
      val response =
        for {
          randomDelay <- random.nextIntBounded(200).map(_.millis)
          response    <- ZIO
                           .foreach(request.getBodyAsString) { str =>
                             putStrLn(s"""SERVER RECEIVED PAYLOAD: webhook: $id $str OK""")
                           }
                           .as(Response.status(Status.OK))
                           .orDie
                           .delay(randomDelay)
        } yield response
      response.uninterruptible
  }

  private lazy val port = 8080

  val randomBehavior: URIO[Random, RandomEndpointBehavior] =
    random.nextIntBounded(3).map {
      case 0 => Normal
      case 1 => Flaky
      case _ => Down
    }

  def run: ZIO[ZEnv, IOException, Unit] =
    UStream.repeatEffect(randomBehavior).foreach { behavior =>
      for {
        _ <- putStrLn(s"Endpoint server behavior: $behavior")
        f <- behavior.start.fork.delay(2.seconds)
        _ <- f.interrupt.delay(1.minute)
      } yield ()
    }
}

object RestartingWebhookServer {

  // JSON webhook event stream
  private lazy val events = UStream
    .iterate(0L)(_ + 1)
    .zip(UStream.repeatEffect(random.nextIntBounded(webhookCount)))
    .map {
      case (i, webhookId) =>
        WebhookEvent(
          WebhookEventKey(WebhookEventId(i), WebhookId(webhookId.toLong)),
          WebhookEventStatus.New,
          s"""{"event":$i}""",
          Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
        )
    }

  private lazy val port = 8080

  def start =
    runServerThenShutdown.forever

  private def runServerThenShutdown =
    for {
      server        <- WebhookServer.start
      _             <- putStrLn("Server started")
      _             <- server.subscribeToErrors
                         .use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
                         .fork
      stopStreaming <- Promise.make[Nothing, Unit]
      _             <- TestWebhookEventRepo.enqueueNew
      _             <- events
                         .schedule(Schedule.spaced(20.micros))
                         .map(Some(_))
                         .mergeTerminateRight(UStream.fromEffect(stopStreaming.await.as(None)))
                         .collectSome
                         .foreach(TestWebhookEventRepo.createEvent)
                         .fork
      duration      <- random.nextIntBetween(3000, 10000).map(_.millis)
      _             <- server.shutdown.delay(duration)
      _             <- putStrLn("Server shut down")
      _             <- stopStreaming.succeed(())
    } yield server

  lazy val testWebhooks = (0 until 250).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtLeastOnce
    )
  } ++ (250 until 500).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce
    )
  } ++ (500 until 750).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtLeastOnce
    )
  } ++ (750 until 1000).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtMostOnce
    )
  }

  private lazy val webhookCount = 1000
}
