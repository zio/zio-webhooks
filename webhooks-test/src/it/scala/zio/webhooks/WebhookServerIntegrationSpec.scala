package zio.webhooks

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.clock.Clock
import zio.console.putStrLn
import zio.duration._
import zio.json._
import zio.magic._
import zio.random.Random
import zio.stream._
import zio.test._
import zio.test.TestAspect.timeout
import zio.webhooks.WebhookServerIntegrationSpecUtil._
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._

object WebhookServerIntegrationSpec extends DefaultRunnableSpec {
  val spec =
    suite("WebhookServerIntegrationSpec")(
      testM("all events are delivered eventually") {
        val n = 10000L // number of events

        (for {
          delivered <- SubscriptionRef.make(Set.empty[Int])
          test       = for {
                         _ <- ZIO.foreach_(testWebhooks)(TestWebhookRepo.setWebhook)
                         _ <- delivered.changes.collect {
                                case set if set.size % 100 == 0 || set.size / n.toDouble >= 0.99 =>
                                  set.size.toString
                              }
                                .foreach(size => putStrLn(s"delivered so far: $size").orDie)
                                .fork
                         _ <- WebhookServer.start.use_ {
                                for {
                                  reliableEndpoint <- httpEndpointServer.start(port, reliableEndpoint(delivered)).fork
                                  // create events for webhooks with single delivery, at-most-once semantics
                                  _                <- singleAtMostOnceEvents(n)
                                                        // pace events so we don't overwhelm the endpoint
                                                        .schedule(Schedule.spaced(50.micros).jittered)
                                                        .foreach(TestWebhookEventRepo.createEvent)
                                  // create events for webhooks with batched delivery, at-most-once semantics
                                  // no need to pace events as batching minimizes requests sent
                                  _                <- batchedAtMostOnceEvents(n).foreach(
                                                        TestWebhookEventRepo.createEvent
                                                      )
                                  // wait to get half
                                  _                <- delivered.changes.filter(_.size == n / 2).runHead
                                  _                <- reliableEndpoint.interrupt
                                } yield ()
                              }
                         // start restarting server and endpoint with random behavior
                         _ <- RestartingWebhookServer.start.fork
                         _ <- RandomEndpointBehavior.run(delivered).fork
                         // send events for webhooks with at-least-once semantics
                         _ <- singleAtLeastOnceEvents(n)
                                .schedule(Schedule.spaced(25.micros))
                                .foreach(TestWebhookEventRepo.createEvent)
                         _ <- batchedAtLeastOnceEvents(n).foreach(TestWebhookEventRepo.createEvent)
                         // wait to get second half
                         _ <- delivered.changes.filter(_.size == n).runHead
                       } yield ()
          // dump repo and events not delivered on timeout
          _         <- test.ensuring(
                         for {
                           deliveredSize <- delivered.ref.get.map(_.size)
                           _             <- TestWebhookEventRepo.dumpEventIds
                                              .map(_.toList.sortBy(_._1))
                                              .debug("all IDs in repo")
                                              .when(deliveredSize < n)
                           _             <- delivered.ref.get
                                              .map(delivered => ((0 until n.toInt).toSet diff delivered).toList.sorted)
                                              .debug("not delivered")
                                              .when(deliveredSize < n)
                         } yield ()
                       )
        } yield assertCompletes)
          .provideSomeLayer[IntegrationEnv](Clock.live ++ console.Console.live ++ random.Random.live)
      } @@ timeout(3.minutes),
      testM("slow subscribers don't slow down the whole server") {
        assertCompletesM
      }
    ).injectCustom(integrationEnv)
}

object WebhookServerIntegrationSpecUtil {

  // limit max backoff to 1 second so tests don't take too long
  def customConfig =
    WebhookServerConfig.default.map { hasConfig =>
      val config = hasConfig.get
      config.copy(
        retry = config.retry.copy(
          maxBackoff = 1.second
        )
      )
    }

  // backport for 2.12
  implicit class EitherOps[A, B](either: Either[A, B]) {

    def orElseThat[A1 >: A, B1 >: B](or: => Either[A1, B1]): Either[A1, B1] =
      either match {
        case Right(_) => either
        case _        => or
      }
  }

  def events(webhookIdRange: (Int, Int)): ZStream[Random, Nothing, WebhookEvent] =
    UStream
      .iterate(0L)(_ + 1)
      .zip(UStream.repeatEffect(random.nextIntBetween(webhookIdRange._1, webhookIdRange._2)))
      .map {
        case (i, webhookId) =>
          WebhookEvent(
            WebhookEventKey(WebhookEventId(i), WebhookId(webhookId.toLong)),
            WebhookEventStatus.New,
            i.toString, // a single number string is valid JSON
            Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
          )
      }

  def singleAtMostOnceEvents(n: Long) =
    events(webhookIdRange = (0, 250)).take(n / 4)

  def batchedAtMostOnceEvents(n: Long) =
    events(webhookIdRange = (250, 500)).drop(n / 4).take(n / 4)

  def singleAtLeastOnceEvents(n: Long) =
    events(webhookIdRange = (500, 750)).drop(n / 2).take(n / 4)

  def batchedAtLeastOnceEvents(n: Long) =
    events(webhookIdRange = (750, 1000)).drop(3 * n / 4).take(n / 4)

  type IntegrationEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[WebhookHttpClient]
    with Has[WebhooksProxy]
    with Has[WebhookServerConfig]
    with Has[SerializePayload]

  // alias for zio-http endpoint server
  lazy val httpEndpointServer = Server

  lazy val integrationEnv: URLayer[Clock, IntegrationEnv] =
    ZLayer
      .wireSome[Clock, IntegrationEnv](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        TestWebhookRepo.test,
        WebhookServerConfig.default,
        WebhookSttpClient.live,
        WebhooksProxy.live
      )
      .orDie

  lazy val port = 8081

  def reliableEndpoint(delivered: SubscriptionRef[Set[Int]]) =
    HttpApp.collectM {
      case request @ Method.POST -> Root / "endpoint" / (id @ _) =>
        for {
          randomDelay <- random.nextIntBounded(200).map(_.millis)
          response    <- ZIO
                           .foreach_(request.getBodyAsString) { body =>
                             val singlePayload = body.fromJson[Int].map(Left(_))
                             val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                             val payload       = singlePayload.orElseThat(batchPayload).toOption
                             ZIO.foreach_(payload) {
                               case Left(i)   =>
                                 delivered.ref.update(set => UIO(set + i))
                               case Right(is) =>
                                 delivered.ref.update(set => UIO(set ++ is))
                             }
                           }
                           .as(Response.status(Status.OK))
                           .delay(randomDelay) // simulate network/server latency
        } yield response
    }

  lazy val testWebhooks: IndexedSeq[Webhook] = (0 until 250).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce
    )
  } ++ (250 until 500).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtMostOnce
    )
  } ++ (500 until 750).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtLeastOnce
    )
  } ++ (750 until 1000).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtLeastOnce
    )
  }

  lazy val webhookCount = 1000
}

sealed trait RandomEndpointBehavior extends Product with Serializable { self =>
  import RandomEndpointBehavior._

  def start(delivered: SubscriptionRef[Set[Int]]) =
    self match {
      case RandomEndpointBehavior.Down  =>
        ZIO.unit
      case RandomEndpointBehavior.Flaky =>
        httpEndpointServer.start(port, flakyBehavior(delivered))
    }
}

object RandomEndpointBehavior {
  case object Down  extends RandomEndpointBehavior
  case object Flaky extends RandomEndpointBehavior

  def flakyBehavior(delivered: SubscriptionRef[Set[Int]]) =
    HttpApp.collectM {
      case request @ Method.POST -> Root / "endpoint" / (id @ _) =>
        for {
          n           <- random.nextIntBounded(100)
          randomDelay <- random.nextIntBounded(200).map(_.millis)
          response    <- ZIO
                           .foreach(request.getBodyAsString) { body =>
                             val singlePayload = body.fromJson[Int].map(Left(_))
                             val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                             val payload       = singlePayload.orElseThat(batchPayload).toOption
                             if (n < 60)
                               ZIO
                                 .foreach_(payload) {
                                   case Left(i)   =>
                                     delivered.ref.update(set => UIO(set + i))
                                   case Right(is) =>
                                     delivered.ref.update(set => UIO(set ++ is))
                                 }
                                 .as(Response.status(Status.OK))
                             else
                               UIO(Response.status(Status.NOT_FOUND))
                           }
                           .delay(randomDelay)
        } yield response.getOrElse(Response.fromHttpError(HttpError.BadRequest("empty body")))
    }

  // just an alias for a zio-http server to tell it apart from the webhook server
  lazy val httpEndpointServer: Server.type = Server

  val normalBehavior = HttpApp.collectM {
    case request @ Method.POST -> Root / "endpoint" / id =>
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
  }

  val randomBehavior: URIO[Random, RandomEndpointBehavior] =
    random.nextIntBounded(100).map(n => if (n < 80) Flaky else Down)

  def run(delivered: SubscriptionRef[Set[Int]]) =
    UStream.repeatEffect(randomBehavior).foreach { behavior =>
      for {
        _ <- putStrLn(s"Endpoint server behavior: $behavior")
        f <- behavior.start(delivered).fork
        _ <- behavior match {

               case Down  =>
                 f.interrupt.delay(2.seconds)
               case Flaky =>
                 f.interrupt.delay(10.seconds)
             }
      } yield ()
    }
}

object RestartingWebhookServer {

  import zio.console.putStrLnErr

  def start =
    runServerThenShutdown.forever

  private def runServerThenShutdown =
    for {
      _ <- putStrLn("Server starting")
      _ <- WebhookServer.start.use { server =>
             for {
               _        <- putStrLn("Server started")
               f        <- server.subscribeToErrors
                             .use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
                             .fork
               _        <- TestWebhookEventRepo.enqueueNew
               duration <- random.nextIntBetween(3000, 5000).map(_.millis)
               _        <- f.interrupt.delay(duration)
             } yield ()
           }
      _ <- putStrLn("Server shut down")
    } yield ()
}
