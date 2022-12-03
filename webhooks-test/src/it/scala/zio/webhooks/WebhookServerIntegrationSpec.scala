package zio.webhooks

import zhttp.http._
import zhttp.service.Server
import zio.Console.{ printError, printLine }
import zio.{ Random, _ }
import zio.json._
import zio.stream._
import zio.test.TestAspect._
import zio.test._
import zio.webhooks.WebhookServerIntegrationSpecUtil._
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.testkit._

object WebhookServerIntegrationSpec extends ZIOSpecDefault {
  val spec =
    suite("WebhookServerIntegrationSpec")(
      test("all events are delivered eventually") {
        val numEvents = 10000L

        def testDelivered(delivered: SubscriptionRef[Set[Int]]) =
          for {
            _ <- ZIO.foreachDiscard(testWebhooks)(TestWebhookRepo.setWebhook)
            _ <- delivered.changes.collect {
                   case set if set.size % 100 == 0 || set.size / numEvents.toDouble >= 0.99 =>
                     set.size.toString
                 }
                   .foreach(size => printLine(s"delivered so far: $size").orDie)
                   .fork
            _ <- ZIO.scoped {
                   WebhookServer.start *> {
                     for {
                       reliableEndpoint <- httpEndpointServer.start(port, reliableEndpoint(delivered)).fork
                       // create events for webhooks with single delivery, at-most-once semantics
                       _                <- singleAtMostOnceEvents(numEvents)
                                             // pace events so we don't overwhelm the endpoint
                                             .schedule(Schedule.spaced(50.micros).jittered)
                                             .foreach(TestWebhookEventRepo.createEvent)
                                             .delay(1000.millis) // give time for endpoint to be ready
                       // create events for webhooks with batched delivery, at-most-once semantics
                       // no need to pace events as batching minimizes requests sent
                       _ <- batchedAtMostOnceEvents(numEvents).foreach(TestWebhookEventRepo.createEvent)
                       // wait to get half
                       _ <- printLine("delivered").orDie
                       _ <- delivered.changes.filter(_.size == numEvents / 2).runHead
                       _ <- printLine("reliableEndpoint").orDie
                       _ <- reliableEndpoint.interrupt.fork
                       _ <- printLine("reliableEndpoint inter").orDie
                     } yield ()
                   }
                 }
            _ <- printLine("RestartingWebhookServer").orDie
            // start restarting server and endpoint with random behavior
            _ <- RestartingWebhookServer.start.fork
            _ <- RandomEndpointBehavior.run(delivered).fork
            // send events for webhooks with at-least-once semantics
            _ <- singleAtLeastOnceEvents(numEvents)
                   .schedule(Schedule.spaced(25.micros))
                   .foreach(TestWebhookEventRepo.createEvent)
            _ <- batchedAtLeastOnceEvents(numEvents).foreach(TestWebhookEventRepo.createEvent)
            // wait to get second half
            _ <- delivered.changes.filter(_.size == numEvents).runHead
          } yield ()

        for {
          delivered <- SubscriptionRef.make(Set.empty[Int])
          _         <- testDelivered(delivered).ensuring(
                         // dump repo and events not delivered on timeout
                         for {
                           deliveredSize <- delivered.get.map(_.size)
                           _             <- TestWebhookEventRepo.dumpEventIds
                                              .map(_.toList.sortBy(_._1))
                                              .debug("all IDs in repo")
                                              .when(deliveredSize < numEvents)
                           _             <- delivered.get
                                              .map(delivered => ((0 until numEvents.toInt).toSet diff delivered).toList.sorted)
                                              .debug("not delivered")
                                              .when(deliveredSize < numEvents)
                         } yield ()
                       )
        } yield assertCompletes
      } @@ timeout(5.minutes) @@ withLiveClock @@ withLiveConsole,
      test("slow subscribers do not slow down fast ones") {
        val webhookCount     = 100
        val eventsPerWebhook = 1000
        val testWebhooks     = (0 until webhookCount).map { i =>
          Webhook(
            id = WebhookId(i.toLong),
            url = s"http://0.0.0.0:$port/endpoint/$i",
            label = s"test webhook $i",
            WebhookStatus.Enabled,
            WebhookDeliveryMode.SingleAtMostOnce,
            None
          )
        }

        // 100 streams with 1000 events each
        val eventStreams =
          ZStream.mergeAllUnbounded()(
            (0L until webhookCount.toLong).map(webhookId =>
              ZStream
                .iterate(0L)(_ + 1)
                .map { eventId =>
                  WebhookEvent(
                    WebhookEventKey(WebhookEventId(eventId), WebhookId(webhookId)),
                    WebhookEventStatus.New,
                    eventId.toString,
                    Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
                    None
                  )
                }
                .take(eventsPerWebhook.toLong)
            ): _*
          )

        for {
          delivered  <- SubscriptionRef.make(Set.empty[Int])
          _          <- delivered.changes.map(_.size).foreach(size => printLine(s"delivered so far: $size").orDie).fork
          _          <- httpEndpointServer.start(port, slowEndpointsExceptFirst(delivered)).fork
          _          <- ZIO.foreachDiscard(testWebhooks)(TestWebhookRepo.setWebhook)
          testResult <- ZIO.scoped {
                          WebhookServer.start.flatMap { server =>
                            for {
                              _ <- server.subscribeToErrors
                                     .flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printError(_)))
                                     .forkScoped
                              _ <- eventStreams.foreach(TestWebhookEventRepo.createEvent).forkScoped
                              _ <- delivered.changes.filter(_.size == eventsPerWebhook).runHead
                            } yield assertCompletes
                          }
                        }
        } yield testResult
      } @@ timeout(5.minutes) @@ TestAspect.withLiveClock @@ TestAspect.withLiveConsole
    ).provide(integrationEnv) @@ sequential
}

object WebhookServerIntegrationSpecUtil {

  // limit max backoff to 1 second so tests don't take too long
  def customConfig =
    WebhookServerConfig.default.update { config =>
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

  def events(webhookIdRange: (Int, Int)): ZStream[Any, Nothing, WebhookEvent] =
    ZStream
      .iterate(0L)(_ + 1)
      .zip(ZStream.repeatZIO(Random.nextIntBetween(webhookIdRange._1, webhookIdRange._2)))
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

  def singleAtMostOnceEvents(n: Long) =
    events(webhookIdRange = (0, 250)).take(n / 4)

  def batchedAtMostOnceEvents(n: Long) =
    events(webhookIdRange = (250, 500)).drop(n.toInt / 4).take(n / 4)

  def singleAtLeastOnceEvents(n: Long) =
    events(webhookIdRange = (500, 750)).drop(n.toInt / 2).take(n / 4)

  def batchedAtLeastOnceEvents(n: Long) =
    events(webhookIdRange = (750, 1000)).drop(3 * n.toInt / 4).take(n / 4)

  type IntegrationEnv = WebhookEventRepo
    with TestWebhookEventRepo
    with WebhookRepo
    with TestWebhookRepo
    with WebhookStateRepo
    with WebhookHttpClient
    with WebhooksProxy
    with WebhookServerConfig
    with SerializePayload

  // alias for zio-http endpoint server
  lazy val httpEndpointServer = Server

  lazy val integrationEnv: ULayer[IntegrationEnv] =
    ZLayer
      .make[IntegrationEnv](
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
    Http.collectZIO[Request] {
      case request @ Method.POST -> !! / "endpoint" / (id @ _) =>
        for {
          randomDelay <- Random.nextIntBounded(200).map(_.millis)
          response    <- request.body.asString.flatMap { body =>
                           val singlePayload = body.fromJson[Int].map(Left(_))
                           val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                           val payload       = singlePayload.orElseThat(batchPayload).toOption
                           ZIO.foreachDiscard(payload) {
                             case Left(i)   =>
                               delivered.updateZIO(set => ZIO.succeed(set + i))
                             case Right(is) =>
                               delivered.updateZIO(set => ZIO.succeed(set ++ is))
                           }
                         }
                           .as(Response.status(Status.Ok))
                           .delay(randomDelay) // simulate network/server latency
        } yield response
    }

  def slowEndpointsExceptFirst(delivered: SubscriptionRef[Set[Int]]) =
    Http.collectZIO[Request] {
      case request @ Method.POST -> !! / "endpoint" / id if id == "0" =>
        for {
          _        <- request.body.asString.flatMap { body =>
                        val singlePayload = body.fromJson[Int].map(Left(_))
                        val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                        val payload       = singlePayload.orElseThat(batchPayload).toOption
                        ZIO
                          .foreachDiscard(payload) {
                            case Left(i)   =>
                              delivered.updateZIO(set => ZIO.succeed(set + i))
                            case Right(is) =>
                              delivered.updateZIO(set => ZIO.succeed(set ++ is))
                          }
                      }
          response <- ZIO.succeed(Response.status(Status.Ok))
        } yield response
      case _                                                          =>
        ZIO.succeed(Response.status(Status.Ok)).delay(1.minute)
    }

  lazy val testWebhooks: IndexedSeq[Webhook] = (0 until 250).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce,
      None
    )
  } ++ (250 until 500).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtMostOnce,
      None
    )
  } ++ (500 until 750).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtLeastOnce,
      None
    )
  } ++ (750 until 1000).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtLeastOnce,
      None
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
    Http.collectZIO[Request] {
      case request @ Method.POST -> !! / "endpoint" / (id @ _) =>
        for {
          n           <- Random.nextIntBounded(100)
          randomDelay <- Random.nextIntBounded(200).map(_.millis)
          response    <- request.body.asString.flatMap { body =>
                           val singlePayload = body.fromJson[Int].map(Left(_))
                           val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                           val payload       = singlePayload.orElseThat(batchPayload).toOption
                           if (n < 60)
                             ZIO
                               .foreachDiscard(payload) {
                                 case Left(i)   =>
                                   delivered.updateZIO(set => ZIO.succeed(set + i))
                                 case Right(is) =>
                                   delivered.updateZIO(set => ZIO.succeed(set ++ is))
                               }
                               .as(Response.status(Status.Ok))
                           else
                             ZIO.succeed(Response.status(Status.NotFound))
                         }
                           .delay(randomDelay)
        } yield response
    }

  // just an alias for a zio-http server to tell it apart from the webhook server
  lazy val httpEndpointServer: Server.type = Server

  val randomBehavior: UIO[RandomEndpointBehavior] =
    Random.nextIntBounded(100).map(n => if (n < 80) Flaky else Down)

  def run(delivered: SubscriptionRef[Set[Int]]) =
    ZStream.repeatZIO(randomBehavior).foreach { behavior =>
      for {
        _ <- printLine(s"Endpoint server behavior: $behavior")
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
                               .flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printError(_)))
                               .forkScoped
                 _        <- TestWebhookEventRepo.enqueueNew
                 duration <- Random.nextIntBetween(3000, 5000).map(_.millis)
                 _        <- f.interrupt.delay(duration)
               } yield ()
             }
           }
      _ <- printLine("Server shut down")
    } yield ()
}
