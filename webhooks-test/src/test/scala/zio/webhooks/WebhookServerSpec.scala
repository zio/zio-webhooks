package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.json._
import zio.magic._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.environment._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServerSpecUtil._
import zio.webhooks.testkit.TestWebhookHttpClient.{ StubResponse, StubResponses }
import zio.webhooks.testkit._

import java.time.Instant

object WebhookServerSpec extends DefaultRunnableSpec {
  val spec =
    suite("WebhookServerSpec")(
      suite("batching disabled")(
        suite("webhooks with at-most-once delivery")(
          testM("dispatches correct request given event") {
            val webhook = singleWebhook(0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload",
              jsonContentHeaders
            )

            val expectedRequest = WebhookHttpRequest(webhook.url, event.content, event.headers)

            webhooksTestScenario(
              stubResponses = UStream(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.take)(equalTo(expectedRequest)))
          },
          testM("webhook stays enabled on dispatch success") {
            val webhook = singleWebhook(0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload",
              jsonContentHeaders
            )

            webhooksTestScenario(
              stubResponses = UStream(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Webhooks
            )((webhooks, _) => assertM(webhooks.take)(equalTo(webhook)))
          },
          testM("event is marked Delivering, then Delivered on successful dispatch") {
            val webhook = singleWebhook(0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload",
              jsonContentHeaders
            )

            val expectedStatuses = List(WebhookEventStatus.Delivering, WebhookEventStatus.Delivered)

            webhooksTestScenario(
              stubResponses = UStream(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Events
            ) { (events, _) =>
              val eventStatuses = events.filterOutput(!_.isNew).map(_.status).takeBetween(2, 3)
              assertM(eventStatuses)(hasSameElements(expectedStatuses))
            }
          },
          testM("can dispatch single event to n webhooks") {
            val n                 = 100
            val webhooks          = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
            val eventsToNWebhooks = webhooks.map(_.id).flatMap(webhook => createPlaintextEvents(1)(webhook))

            webhooksTestScenario(
              stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = webhooks,
              events = eventsToNWebhooks,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
          },
          testM("dispatches no events for disabled webhooks") {
            val n       = 100
            val webhook = singleWebhook(0, WebhookStatus.Disabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )((requests, _) => requests.take *> assertCompletesM)
          } @@ timeout(50.millis) @@ failing,
          testM("dispatches no events for unavailable webhooks") {
            val n       = 100
            val webhook =
              singleWebhook(0, WebhookStatus.Unavailable(Instant.EPOCH), WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )((requests, _) => requests.take *> assertCompletesM)
          } @@ timeout(50.millis) @@ failing,
          testM("doesn't batch when no batching configuration is given") {
            val n       = 100
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
          },
          testM("a webhook receiver returning non-200 fails events") {
            val n       = 100
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Right(WebhookHttpResponse(404))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Events
            ) { (events, _) =>
              assertM(
                events.map(_.status).filterOutput(_ == WebhookEventStatus.Failed).takeBetween(n, n + 1)
              )(hasSize(equalTo(n)))
            }
          },
          testM("missing webhook errors are published") {
            val idRange               = 401L to 404L
            val missingWebhookIds     = idRange.map(WebhookId(_))
            val eventsMissingWebhooks = missingWebhookIds.flatMap(id => createPlaintextEvents(1)(id))

            val expectedErrorCount = missingWebhookIds.size

            webhooksTestScenario(
              stubResponses = UStream(Right(WebhookHttpResponse(200))),
              webhooks = List.empty,
              events = eventsMissingWebhooks,
              ScenarioInterest.Errors
            ) { (errors, _) =>
              assertM(errors.takeBetween(expectedErrorCount, expectedErrorCount + 1))(
                hasSameElements(idRange.map(id => MissingWebhookError(WebhookId(id))))
              )
            }
          },
          testM("bad webhook URL errors are published") {
            assertCompletesM
          } @@ ignore
        ),
        suite("webhooks with at-least-once delivery")(
          testM("immediately retries once on non-200 response") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(Right(WebhookHttpResponse(500)), Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
          },
          testM("immediately retries once on IOException") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(Left(None), Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
          },
          testM("retries until success before 7-day retry timeout") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(Left(None), Left(None), Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            ) { (requests, _) =>
              for {
                request1 <- requests.take.as(true)
                request2 <- requests.take.as(true)
                _        <- TestClock.adjust(10.millis)
                request3 <- requests.take.as(true)
              } yield assertTrue(request1 && request2 && request3)
            }
          },
          testM("webhook is set unavailable after 7-day retry timeout") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Left(None)),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Webhooks
            ) { (webhooks, _) =>
              for {
                status  <- webhooks.take.map(_.status)
                status2 <- webhooks.take.map(_.status) raceEither TestClock.adjust(7.days).forever
              } yield assertTrue(status == WebhookStatus.Enabled) &&
                assert(status2)(isLeft(isSubtype[WebhookStatus.Unavailable](Assertion.anything)))
            }
          },
          testM("marks all a webhook's events failed when marked unavailable") {
            val n       = 2
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(n)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Left(None)),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Events
            ) { (events, _) =>
              UStream
                .fromQueue(events)
                .filter(_.status == WebhookEventStatus.Failed)
                .take(n.toLong)
                .mergeTerminateLeft(UStream.repeatEffect(TestClock.adjust(7.days)))
                .runDrain *> assertCompletesM
            }
          },
          testM("retries past first one back off exponentially") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses =
                UStream.fromIterable(List.fill(5)(Left(None))) ++ UStream(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            ) {
              (requests, _) =>
                for {
                  _ <- requests.take // 1st failure
                  _ <- requests.take // 1st retry immediately after
                  _ <- TestClock.adjust(10.millis)
                  _ <- requests.take // 2nd retry after 10ms
                  _ <- TestClock.adjust(20.millis)
                  _ <- requests.take // 3rd retry after 20ms
                  _ <- TestClock.adjust(40.millis)
                  _ <- requests.take // 4th retry after 40ms
                  _ <- TestClock.adjust(80.millis)
                  _ <- requests.take // 5th retry after 80ms
                } yield assertCompletes
            }
          },
          testM("doesn't retry requests after requests succeed again") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(3)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(Left(None)) ++ UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(4, 5))(hasSize(equalTo(4))))
          },
          testM("retries for multiple webhooks") {
            val n                 = 100
            val webhooks          = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val eventsToNWebhooks = webhooks.map(_.id).map { webhookId =>
              WebhookEvent(
                WebhookEventKey(WebhookEventId(0), webhookId),
                WebhookEventStatus.New,
                webhookId.value.toString,
                Chunk(("Accept", "*/*"), ("Content-Type", "text/plain"))
              )
            }

            val expectedCount = n * 2

            for {
              queues     <- ZIO.collectAll(Chunk.fill(100)(Queue.bounded[StubResponse](2)))
              _          <- ZIO.collectAll(queues.map(_.offerAll(List(Left(None), Right(WebhookHttpResponse(200))))))
              testResult <- webhooksTestScenario(
                              stubResponses = request => queues.lift(request.content.toInt),
                              webhooks = webhooks,
                              events = eventsToNWebhooks,
                              ScenarioInterest.Requests
                            ) { requests =>
                              assertM(requests.takeBetween(expectedCount, expectedCount + 1))(
                                hasSize(equalTo(expectedCount))
                              )
                            }
            } yield testResult
          }
        )
      ).injectSome[TestEnvironment](specEnv, WebhookServerConfig.default),
      suite("batching enabled")(
        testM("batches events queued up since last request") {
          val n       = 100
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val batches = createPlaintextEvents(n)(webhook.id).grouped(10).toList

          val expectedRequestsMade = 10

          webhooksTestScenario(
            stubResponses = UStream.empty,
            webhooks = List(webhook),
            events = Iterable.empty,
            ScenarioInterest.Requests
          ) { (requests, responseQueue) =>
            val actualRequests = ZIO.foreach(batches) { batch =>
              for {
                _       <- ZIO.foreach_(batch)(TestWebhookEventRepo.createEvent)
                _       <- responseQueue.offer(Right(WebhookHttpResponse(200)))
                request <- requests.take
              } yield request
            }
            assertM(actualRequests)(hasSize(equalTo(expectedRequestsMade)))
          }
        },
        testM("batches for multiple webhooks") {
          val eventCount   = 100
          val webhookCount = 10
          val webhooks     = createWebhooks(webhookCount)(
            WebhookStatus.Enabled,
            WebhookDeliveryMode.BatchedAtMostOnce
          )
          val events       = webhooks.map(_.id).flatMap { webhookId =>
            createPlaintextEvents(eventCount / webhookCount)(webhookId)
          }

          val minRequestsMade = eventCount / webhookCount // 10
          val maxRequestsMade = minRequestsMade * 2

          webhooksTestScenario(
            stubResponses = UStream.empty,
            webhooks = webhooks,
            events = events,
            ScenarioInterest.Requests
          ) { (requests, responseQueue) =>
            for {
              _        <- responseQueue.offerAll(List.fill(10)(Right(WebhookHttpResponse(200))))
              requests <- requests.takeBetween(minRequestsMade, minRequestsMade + 1)
            } yield assertTrue((minRequestsMade <= requests.size) && (requests.size <= maxRequestsMade))
          }
        },
        testM("events dispatched by batch are marked delivered") {
          val n          = 100
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val batchCount = 10
          val testEvents = createPlaintextEvents(n)(webhook.id).grouped(batchCount).toList

          webhooksTestScenario(
            stubResponses = UStream.empty,
            webhooks = List(webhook),
            events = Iterable.empty,
            ScenarioInterest.Events
          ) { (events, responseQueue) =>
            for {
              _               <- ZIO.foreach_(testEvents) {
                                   ZIO.foreach_(_)(TestWebhookEventRepo.createEvent) *>
                                     responseQueue.offer(Right(WebhookHttpResponse(200)))
                                 }
              deliveredEvents <- events
                                   .filterOutput(_.status == WebhookEventStatus.Delivered)
                                   .takeBetween(batchCount, n)
            } yield assertTrue(batchCount <= deliveredEvents.size && deliveredEvents.size <= n)
          }
        },
        testM("batches events on webhook and content-type") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          val jsonEvents      = createJsonEvents(4)(webhook.id)
          val plaintextEvents = createPlaintextEvents(4)(webhook.id)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents ++ plaintextEvents,
            ScenarioInterest.Requests
          )((requests, _) => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
        },
        testM("JSON event contents are batched into a JSON array") {
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val jsonEvents = createJsonEvents(100)(webhook.id)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents,
            ScenarioInterest.Requests
          ) { (requests, _) =>
            for (content <- requests.take.map(_.content))
              yield {
                val n              = "event".r.findAllMatchIn(content).size
                val appendedJson   = (0 until n).map(i => s"""{"event":"payload$i"}""").mkString(",")
                val expectedOutput = if (n > 1) "[" + appendedJson + "]" else appendedJson
                assertTrue(content == expectedOutput)
              }
          }
        },
        testM("batched plain text event contents are concatenated") {
          val n               = 2
          val webhook         = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val plaintextEvents = createPlaintextEvents(n)(webhook.id)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = plaintextEvents,
            ScenarioInterest.Requests
          ) { (requests, _) =>
            for (content <- requests.take.map(_.content))
              yield {
                val n              = "event".r.findAllMatchIn(content).size
                val expectedOutput = (0 until n).map(i => s"event payload $i").mkString
                assertTrue(content == expectedOutput)
              }
          }
        }
      ).injectSome[TestEnvironment](specEnv, WebhookServerConfig.defaultWithBatching),
      suite("manual server start and shutdown")(
        suite("on shutdown")(
          testM("takes no new events on shut down right after startup") {
            val webhook   = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val testEvent = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "event payload",
              plaintextContentHeaders
            )

            TestWebhookEventRepo.subscribeToEvents.map(_.filterOutput(_.status == WebhookEventStatus.Delivering)).use {
              events =>
                for {
                  responses <- Queue.unbounded[StubResponse]
                  server    <- WebhookServer.create
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Right(WebhookHttpResponse(200)), Right(WebhookHttpResponse(200))))
                  _         <- server.start
                  _         <- server.shutdown
                  _         <- TestWebhookRepo.createWebhook(webhook)
                  _         <- TestWebhookEventRepo.createEvent(testEvent)
                  take      <- events.take.timeout(1.second).provideLayer(Clock.live)
                } yield assertTrue(take.isEmpty)
            }
          },
          testM("stops subscribing to new events") {
            val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val testEvents = createPlaintextEvents(2)(WebhookId(0))

            TestWebhookEventRepo.subscribeToEvents.map(_.filterOutput(_.status == WebhookEventStatus.Delivering)).use {
              events =>
                for {
                  responses <- Queue.unbounded[StubResponse]
                  server    <- WebhookServer.create
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Right(WebhookHttpResponse(200)), Right(WebhookHttpResponse(200))))
                  _         <- server.start
                  _         <- TestWebhookRepo.createWebhook(webhook)
                  _         <- TestWebhookEventRepo.createEvent(testEvents(0))
                  event1    <- events.take.as(true)
                  _         <- server.shutdown
                  _         <- TestWebhookEventRepo.createEvent(testEvents(1))
                  take      <- events.take.timeout(1.second).provideLayer(Clock.live)
                } yield assertTrue(event1 && take.isEmpty)
            }
          },
          testM("retry state is saved") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val event   = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "event payload",
              plaintextContentHeaders
            )

            TestWebhookHttpClient.getRequests.use {
              requests =>
                for {
                  responses <- Queue.unbounded[StubResponse]
                  server    <- WebhookServer.create
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Left(None), Left(None)))
                  _         <- server.start
                  _         <- TestWebhookRepo.createWebhook(webhook)
                  _         <- TestWebhookEventRepo.createEvent(event)
                  _         <- requests.takeN(2) // wait for 2 requests to come through
                  _         <- server.shutdown
                  saveState <- WebhookStateRepo.getState
                                 .repeatUntil(_.isDefined)
                                 .map {
                                   _.map(_.fromJson[PersistentServerState])
                                     .toRight("No save-state")
                                     .flatMap(Predef.identity)
                                 }
                } yield assertTrue(saveState.isRight)
            }
          }
        ),
        suite("on restart")(
          testM("continues persisted retries") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val event   = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "event content",
              plaintextContentHeaders
            )

            TestWebhookHttpClient.getRequests.use {
              requests =>
                for {
                  responses <- Queue.unbounded[StubResponse]
                  server    <- WebhookServer.create
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Left(None), Left(None), Right(WebhookHttpResponse(200))))
                  _         <- server.start
                  _         <- TestWebhookRepo.createWebhook(webhook)
                  _         <- TestWebhookEventRepo.createEvent(event)
                  _         <- requests.takeN(2)
                  _         <- server.shutdown
                  server    <- WebhookServer.create
                  _         <- server.start
                  _         <- requests.take
                  _         <- server.shutdown
                } yield assertCompletes
            }
          },
          testM("resumes timeout duration for retries") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val event   = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "event content",
              plaintextContentHeaders
            )

            (TestWebhookHttpClient.getRequests zip TestWebhookRepo.getWebhooks).use {
              case (requests, webhooks) =>
                for {
                  responses  <- Queue.unbounded[StubResponse]
                  server     <- WebhookServer.create
                  _          <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _          <- responses.offerAll(List(Left(None), Left(None), Right(WebhookHttpResponse(200))))
                  _          <- server.start
                  _          <- TestWebhookRepo.createWebhook(webhook)
                  _          <- TestWebhookEventRepo.createEvent(event)
                  _          <- requests.takeN(2)
                  _          <- TestClock.adjust(3.days)
                  _          <- server.shutdown
                  server     <- WebhookServer.create
                  _          <- server.start
                  _          <- TestClock.adjust(4.days)
                  lastStatus <- webhooks.takeN(2).map(_.last.status)
                  _          <- requests.take
                } yield assert(lastStatus)(isSubtype[WebhookStatus.Unavailable](anything))
            }
          }
          // TODO: test continues retrying for multiple webhooks
          // TODO: test retries eventually get delivered  (property test)
          // TODO: test batched retries eventually get delivered (property test)
        )
      ).injectSome[TestEnvironment](mockEnv, WebhookServerConfig.default)
      // TODO: write webhook status change tests
    ) @@ timeout(20.seconds)
}

object WebhookServerSpecUtil {

  def createWebhooks(n: Int)(status: WebhookStatus, deliveryMode: WebhookDeliveryMode): Iterable[Webhook] =
    (0 until n).map(i => singleWebhook(i.toLong, status, deliveryMode))

  def createJsonEvents(n: Int)(webhookId: WebhookId): Iterable[WebhookEvent] =
    (0 until n).map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i.toLong), webhookId),
        WebhookEventStatus.New,
        s"""{"event":"payload$i"}""",
        jsonContentHeaders
      )
    }

  def createPlaintextEvents(n: Int)(webhookId: WebhookId): IndexedSeq[WebhookEvent] =
    (0 until n).map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i.toLong), webhookId),
        WebhookEventStatus.New,
        "event payload " + i,
        plaintextContentHeaders
      )
    }

  val jsonContentHeaders: Chunk[(String, String)] = Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))

  type MockEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]

  lazy val mockEnv: ZLayer[Clock with Has[WebhookServerConfig], Nothing, MockEnv] =
    ZLayer
      .fromSomeMagic[Clock with Has[WebhookServerConfig], MockEnv](
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookHttpClient.test
      )

  val plaintextContentHeaders: Chunk[(String, String)] = Chunk(("Accept", "*/*"), ("Content-Type", "text/plain"))

  sealed trait ScenarioInterest[A]
  object ScenarioInterest {
    case object Errors   extends ScenarioInterest[WebhookError]
    case object Events   extends ScenarioInterest[WebhookEvent]
    case object Requests extends ScenarioInterest[WebhookHttpRequest]
    case object Webhooks extends ScenarioInterest[Webhook]

    final def dequeueFor[A](scenarioInterest: ScenarioInterest[A]): URManaged[SpecEnv, Dequeue[A]] =
      scenarioInterest match {
        case ScenarioInterest.Errors   =>
          ZManaged.service[WebhookServer].flatMap(_.getErrors)
        case ScenarioInterest.Events   =>
          TestWebhookEventRepo.subscribeToEvents
        case ScenarioInterest.Requests =>
          TestWebhookHttpClient.getRequests
        case ScenarioInterest.Webhooks =>
          TestWebhookRepo.getWebhooks
      }
  }

  def singleWebhook(id: Long, status: WebhookStatus, deliveryMode: WebhookDeliveryMode): Webhook =
    Webhook(
      WebhookId(id),
      "http://example.org/" + id,
      "testWebhook" + id,
      status,
      deliveryMode
    )

  type SpecEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]
    with Has[WebhookServer]

  lazy val specEnv: URLayer[Clock with Has[WebhookServerConfig], SpecEnv] =
    ZLayer
      .fromSomeMagic[Clock with Has[WebhookServerConfig], SpecEnv](
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookHttpClient.test,
        WebhookServer.live
      )

  type TestServerEnv = Has[WebhookRepo]
    with Has[WebhookStateRepo]
    with Has[WebhookEventRepo]
    with Has[WebhookHttpClient]
    with Clock

  // TODO: keep an eye on the duplication here
  def webhooksTestScenario[A](
    stubResponses: WebhookHttpRequest => StubResponses,
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A]
  )(
    assertion: Dequeue[A] => URIO[TestClock, TestResult]
  ): URIO[SpecEnv with TestClock with Has[WebhookServer] with Clock, TestResult] =
    ScenarioInterest.dequeueFor(scenarioInterest).map(assertion).flatMap(_.forkManaged).use { testFiber =>
      for {
        _          <- TestWebhookHttpClient.setResponse(stubResponses)
        _          <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook)
        _          <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
        testResult <- testFiber.join
      } yield testResult
    }

  def webhooksTestScenario[A](
    stubResponses: UStream[StubResponse],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A]
  )(
    assertion: (Dequeue[A], Queue[StubResponse]) => URIO[SpecEnv with TestClock, TestResult]
  ): URIO[SpecEnv with TestClock with Has[WebhookServer] with Clock, TestResult] =
    ScenarioInterest.dequeueFor(scenarioInterest).use { dequeue =>
      for {
        responseQueue <- Queue.bounded[StubResponse](1)
        testFiber     <- assertion(dequeue, responseQueue).fork
        _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
        _             <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook)
        _             <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
        _             <- stubResponses.run(ZSink.fromQueue(responseQueue)).fork
        testResult    <- testFiber.join
      } yield testResult
    }
}
