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
import zio.webhooks.testkit._

import java.io.IOException

/**
 * Runs an example that simulates a comprehensive suite of scenarios that may occur during the
 * operation of a webhook server.
 */
object ComprehensiveExample extends App {
  // JSON webhook event stream
  private lazy val nEvents = UStream
    .iterate(0L)(_ + 1).zip(UStream.repeatEffect(random.nextIntBounded(webhookCount)))
    .map { case (i, webhookId) =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i), WebhookId(webhookId.toLong)),
        WebhookEventStatus.New,
        s"""{"event":$i}""",
        Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
      )
    }

  private lazy val port = 8080

  private def program =
    for {
      _ <- EndpointBehavior.run.fork
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_))).fork
      _ <- ZIO.foreach_(webhooks)(TestWebhookRepo.setWebhook)
      _ <- nEvents/*.schedule(Schedule.spaced(50.micros))*/.foreach(TestWebhookEventRepo.createEvent)
    } yield ()

  /**
   * The webhook server is started as part of the layer construction. See [[WebhookServer.live]].
   */
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
        WebhookServer.live,
        WebhooksProxy.live
      )
      .exitCode

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

sealed trait EndpointBehavior extends Product with Serializable { self =>
  import EndpointBehavior._

  def start: ZIO[ZEnv, Throwable, Any] =
    self match {
      case EndpointBehavior.Down   =>
        ZIO.unit
      case EndpointBehavior.Flaky  =>
        httpEndpointServer.start(port, flakyBehavior)
      case EndpointBehavior.Normal =>
        httpEndpointServer.start(port, normalBehavior)
    }
}

object EndpointBehavior {
  case object Down   extends EndpointBehavior
  case object Flaky  extends EndpointBehavior
  case object Normal extends EndpointBehavior

  val flakyBehavior = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" / id =>
      val payload  = request.getBodyAsString
      val response =
        for {
          n          <- random.nextIntBounded(100)
          timeString <- clock.instant.map(_.toString).map(ts => s"[$ts]: ")
          response   <- ZIO
                          .foreach(payload) { payload =>
                            val line = s"$timeString webhook $id $payload"
                            if (n < 60)
                              putStrLn(line + " Response: OK") *> UIO(Response.status(Status.OK))
                            else
                              putStrLn(line + " Response: NOT_FOUND") *> UIO(Response.status(Status.NOT_FOUND))
                          }
                          .orDie
        } yield response.getOrElse(Response.fromHttpError(HttpError.BadRequest("empty body")))
      response.uninterruptible
  }

  // just an alias for a zio-http server to tell it apart from the webhook server
  lazy val httpEndpointServer: Server.type = Server

  val normalBehavior = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" / id =>
      val response = ZIO
        .foreach(request.getBodyAsString)(str => putStrLn(s"""SERVER RECEIVED PAYLOAD: webhook: $id $str OK"""))
        .as(Response.status(Status.OK))
        .orDie
      response.uninterruptible
  }

  private lazy val port = 8080

  val randomBehavior: URIO[Random, EndpointBehavior] =
    random.nextIntBounded(3).map {
      case 0 => Normal
      case 1 => Flaky
      case 2 => Down
      case _ => ??? // impossible with nextIntBounded(3)
    }

  def run: ZIO[zio.ZEnv, IOException, Unit] =
    UStream.repeatEffect(randomBehavior).foreach { behavior =>
      for {
        _ <- putStrLn(s"Endpoint server behavior: $behavior")
        f <- behavior.start.fork
        _ <- f.interrupt.delay(1.minute)
      } yield ()
    }
}

sealed trait ServerBehavior extends Product with Serializable
object ServerBehavior {
  case object Failure extends ServerBehavior
  case object Normal  extends ServerBehavior
  case object Restart extends ServerBehavior
}
