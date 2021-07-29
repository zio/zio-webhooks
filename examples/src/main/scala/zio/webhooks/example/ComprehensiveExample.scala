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
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.example.RestartingWebhookServer.startServer
import zio.webhooks.testkit._

import java.io.IOException

/**
 * Runs an example that simulates a comprehensive suite of scenarios that may occur during the
 * operation of a webhook server.
 */
object ComprehensiveExample extends App {

  private def program =
    for {
      _ <- RestartingWebhookServer.start.fork
      _ <- RandomEndpointBehavior.run.fork
      _ <- clock.sleep(Duration.Infinity)
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectCustom(
        TestWebhookEventRepo.test,
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        WebhookSttpClient.live,
        WebhookServerConfig.default,
        WebhookServerConfig.dispatchConcurrency,
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

sealed trait RestartingWebhookServer extends Product with Serializable { self =>
  def run(ref: Ref[Fiber.Runtime[Nothing, WebhookServer]]) =
    self match {
      case RestartingWebhookServer.Restart =>
        for {
          fiber  <- ref.get
          server <- fiber.join
          _      <- server.shutdown
          fiber  <- startServer.fork
          _      <- ref.set(fiber)
        } yield ()
      case RestartingWebhookServer.Failure =>
        for {
          fiber <- ref.get
          _     <- fiber.interrupt // kill server fiber tree
          fiber <- startServer.fork
          _     <- ref.set(fiber)
        } yield ()
    }
}

object RestartingWebhookServer {
  case object Failure extends RestartingWebhookServer
  case object Restart extends RestartingWebhookServer

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

  private lazy val randomRestart: URIO[Random, Option[RestartingWebhookServer]] =
    random.nextIntBounded(3).map {
      case 0 => Some(Restart)
      case 1 => Some(Failure)
      case _ => None
    }

  def start =
    for {
      _     <- putStrLn(s"Starting webhook server")
      fiber <- startServer.fork
      ref   <- Ref.make(fiber)
      _     <- UStream.repeatEffect(randomRestart).schedule(Schedule.spaced(10.seconds)).foreach { behavior =>
                 putStrLn(s"Webhook server restart: $behavior") *>
                   ZIO.foreach_(behavior)(_.run(ref)).delay(2.second)
               }
    } yield ()

  private def startServer =
    for {
      server <- WebhookServer.start
      _      <- server.subscribeToErrors
                  .use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
                  .fork
      _      <- ZIO.foreach_(webhooks)(TestWebhookRepo.setWebhook)
      _      <- events.schedule(Schedule.spaced(12.micros)).foreach(TestWebhookEventRepo.createEvent)
    } yield server

  private lazy val webhooks = (0 until 250).map { i =>
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
