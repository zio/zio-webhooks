package zio.webhooks

import zio.Console.{ printError, printLine }
import zio._
import zio.stream._
import zio.test.TestAspect._
import zio.test._
import zio.webhooks.WebhookServerIntegrationSpecUtil._
import zio.webhooks.testkit._

object WebhookServerIntegrationSpec extends ZIOSpecDefault {
  val numEvents = 10000L

  val spec =
    suite("WebhookServerIntegrationSpec")(
      test("all events are delivered eventually") {
        def testDelivered(delivered: SubscriptionRef[Set[Int]]) =
          for {
            _ <- ZIO.foreachDiscard(testWebhooks)(TestWebhookRepo.setWebhook)
            _ <- logDeliveredSoFar(delivered).fork
            _ <- ZIO.scoped {
                   for {
                     _                <- WebhookServer.start
                     reliableEndpoint <- httpEndpointServer.start(port, reliableEndpoint(delivered)).fork
                     _                <- singleAtMostOnceEvents(numEvents)
                                           // pace events so we don't overwhelm the endpoint
                                           .schedule(Schedule.spaced(50.micros).jittered)
                                           .foreach(TestWebhookEventRepo.createEvent)
                                           .delay(2.seconds) // give the reliable endpoint time to get ready
                     // no need to pace events as batching minimizes requests sent
                     _ <- batchedAtMostOnceEvents(numEvents).foreach(TestWebhookEventRepo.createEvent)
                     // wait to get half
                     _ <- delivered.changes.filter(_.size == numEvents / 2).runHead
                     _ <- reliableEndpoint.interrupt.fork
                   } yield ()
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
          _         <- testDelivered(delivered).ensuring(dumpRepoAndUndeliveredEvents(delivered))
        } yield assertCompletes
      } @@ timeout(5.minutes) @@ withLiveClock @@ withLiveConsole,
      test("slow subscribers do not slow down fast ones") {
        val setup = SlowWebhookSetup(100, 1000)
        import setup._

        for {
          delivered  <- SubscriptionRef.make(Set.empty[Int])
          _          <- delivered.changes.map(_.size).foreach(size => printLine(s"delivered so far: $size").orDie).fork
          _          <- httpEndpointServer.start(port, slowEndpointsExceptFirst(delivered)).fork
          _          <- ZIO.foreachDiscard(testWebhooks)(TestWebhookRepo.setWebhook)
          testResult <- ZIO.scoped {
                          for {
                            server <- WebhookServer.start
                            _      <- server.subscribeToErrors
                                        .flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printError(_)))
                                        .forkScoped
                            _      <- eventStreams.foreach(TestWebhookEventRepo.createEvent).delay(2.seconds).forkScoped
                            _      <- delivered.changes.filter(_.size == eventsPerWebhook).runHead
                          } yield assertCompletes
                        }
        } yield testResult
      } @@ timeout(7.minutes) @@ TestAspect.withLiveClock @@ TestAspect.withLiveConsole
    ).provide(integrationEnv) @@ sequential

  private def dumpRepoAndUndeliveredEvents(delivered: SubscriptionRef[Set[RuntimeFlags]]) =
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

  private def logDeliveredSoFar(delivered: SubscriptionRef[Set[RuntimeFlags]]) =
    delivered.changes.collect {
      case set if set.size % 100 == 0 || set.size / numEvents.toDouble >= 0.99 =>
        set.size.toString
    }.foreach(size => printLine(s"delivered so far: $size").orDie)
}
