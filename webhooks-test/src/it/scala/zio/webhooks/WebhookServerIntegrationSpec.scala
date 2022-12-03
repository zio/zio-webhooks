package zio.webhooks

import zio.Console.{ printError, printLine }
import zio._
import zio.stream._
import zio.test.TestAspect._
import zio.test._
import zio.webhooks.WebhookServerIntegrationSpecUtil._
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
                              _ <- eventStreams.foreach(TestWebhookEventRepo.createEvent).delay(1.second).forkScoped
                              _ <- delivered.changes.filter(_.size == eventsPerWebhook).runHead
                            } yield assertCompletes
                          }
                        }
        } yield testResult
      } @@ timeout(10.minutes) @@ TestAspect.withLiveClock @@ TestAspect.withLiveConsole
    ).provide(integrationEnv) @@ sequential
}
