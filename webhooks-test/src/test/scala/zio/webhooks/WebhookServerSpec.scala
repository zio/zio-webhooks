package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.magic._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.environment._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServerSpecUtil._
import zio.webhooks.WebhookStatus._
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
              stubResponses = UStream(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Requests
            )(requests => assertM(requests.take)(equalTo(expectedRequest)))
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
              stubResponses = UStream(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Webhooks
            )(webhooks => assertM(webhooks.take)(equalTo(webhook)))
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
              stubResponses = UStream(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Events
            ) { events =>
              val eventStatuses = events.filterOutput(!_.status.isNew).map(_.status).takeBetween(2, 3)
              assertM(eventStatuses)(hasSameElements(expectedStatuses))
            }
          },
          testM("can dispatch single event to n webhooks") {
            val n                 = 100
            val webhooks          = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
            val eventsToNWebhooks = webhooks.map(_.id).flatMap(webhook => createPlaintextEvents(1)(webhook))

            webhooksTestScenario(
              stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
              webhooks = webhooks,
              events = eventsToNWebhooks,
              ScenarioInterest.Requests
            )(requests => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
          },
          testM("dispatches no events for disabled webhooks") {
            val n       = 100
            val webhook = singleWebhook(0, WebhookStatus.Disabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )(_.take *> assertCompletesM)
          } @@ timeout(50.millis) @@ failing,
          testM("dispatches no events for unavailable webhooks") {
            val n       = 100
            val webhook =
              singleWebhook(0, WebhookStatus.Unavailable(Instant.EPOCH), WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )(_.take *> assertCompletesM)
          } @@ timeout(50.millis) @@ failing,
          testM("doesn't batch when no batching configuration is given") {
            val n       = 100
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )(requests => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
          },
          testM("a webhook receiver returning non-200 fails events") {
            val n       = 100
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = UStream.repeat(Some(WebhookHttpResponse(404))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Events
            ) { events =>
              assertM(
                events.map(_.status).filterOutput(_ == WebhookEventStatus.Failed).takeBetween(n, n + 1)
              )(hasSize(equalTo(n)))
            }
          },
          testM("missing webhook errors are published") {
            val idRange               = 401L to 404L
            val missingWebhookIds     = idRange.map(WebhookId)
            val eventsMissingWebhooks = missingWebhookIds.flatMap(id => createPlaintextEvents(1)(id))

            val expectedErrorCount = missingWebhookIds.size

            webhooksTestScenario(
              stubResponses = UStream(Some(WebhookHttpResponse(200))),
              webhooks = List.empty,
              events = eventsMissingWebhooks,
              ScenarioInterest.Errors
            ) { errors =>
              assertM(errors.takeBetween(expectedErrorCount, expectedErrorCount + 1))(
                hasSameElements(idRange.map(id => MissingWebhookError(WebhookId(id))))
              )
            }
          }
        ),
        suite("webhooks with at-least-once delivery")(
          testM("immediately retries once on non-200 response") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(Some(WebhookHttpResponse(500)), Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )(requests => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
          },
          testM("immediately retries once on IOException") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(None, Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )(requests => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
          },
          testM("retrying sets webhook status to Retrying, then Enabled on success") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(None, Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Webhooks
            ) { webhooks =>
              assertM(webhooks.takeN(3).map(_.drop(1).map(_.status)))(
                hasSameElements(List(WebhookStatus.Retrying(Instant.EPOCH), WebhookStatus.Enabled))
              )
            }
          },
          testM("retries until success before 7-day retry timeout") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(None, None, Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            ) { requests =>
              for {
                request1 <- requests.take.as(true)
                request2 <- requests.take.as(true)
                _        <- TestClock.adjust(10.millis)
                request3 <- requests.take.as(true)
              } yield assertTrue(request1 && request2 && request3)
            }
          },
          testM("webhook is set unavailable after 7-day retry timeout") {
            val webhook     = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events      = createPlaintextEvents(1)(webhook.id)
            val timeElapsed = 7.days

            webhooksTestScenario(
              stubResponses = UStream.repeat(None),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Webhooks
            ) { webhooks =>
              for {
                status2 <- webhooks.take *> webhooks.take.map(_.status)
                _       <- TestClock.adjust(timeElapsed)
                status3 <- webhooks.take.map(_.status)
              } yield assertTrue(status2 == Retrying(Instant.EPOCH)) &&
                assertTrue(status3 == Unavailable(Instant.EPOCH.plus(timeElapsed)))
            }
          },
          testM("marks all a webhook's events failed when marked unavailable") {
            val n       = 10
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(n)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream.repeat(None),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Events
            ) {
              events =>
                val pollNext      = events.poll <* TestClock.adjust(1.day)
                val schedule      = Schedule.recurUntil[Option[WebhookEvent]](
                  _.exists(_.status == WebhookEventStatus.Failed)
                ) && Schedule.spaced(10.millis)
                val nFailedEvents = List.fill(n)(pollNext.repeat(schedule).map(_._1))
                ZIO
                  .collectAll(nFailedEvents)
                  .map(_.collect { case Some(event) => event })
                  .provideSomeLayer[TestClock](Clock.live) *> assertCompletesM
            }
          },
          testM("retries past first one backs off exponentially") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream.repeat(None).take(7) ++ UStream(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            ) {
              requests =>
                for {
                  request1 <- requests.take.as(true)
                  request2 <- requests.take.as(true)
                  _        <- TestClock.adjust(10.millis)
                  request3 <- requests.take.as(true)
                  _        <- TestClock.adjust(10.millis)
                  request4 <- requests.poll
                  _        <- TestClock.adjust(10.millis)
                  request5 <- requests.take.as(true)
                  _        <- TestClock.adjust(10.millis)
                  request6 <- requests.poll
                  _        <- TestClock.adjust(30.millis)
                  request7 <- requests.take.as(true)
                } yield assertTrue(request1 && request2 && request3 && request5 && request7) &&
                  assertTrue(request4.isEmpty && request6.isEmpty)
            }
          },
          testM("doesn't retry requests after requests succeed again") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(3)(webhook.id)

            webhooksTestScenario(
              stubResponses = UStream(None) ++ UStream.repeat(Some(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )(requests => assertM(requests.takeBetween(4, 5))(hasSize(equalTo(4))))
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
              queues     <- ZIO.collectAll(Chunk.fill(100)(Queue.bounded[Option[WebhookHttpResponse]](2)))
              _          <- ZIO.collectAll(queues.map(_.offerAll(List(None, Some(WebhookHttpResponse(200))))))
              testResult <- webhooksTestScenario(
                              stubResponses = request => queues.lift(request.content.toInt),
                              webhooks = webhooks,
                              events = eventsToNWebhooks,
                              ScenarioInterest.Requests,
                              adjustDuration = None
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
        testM("batches events by max batch size") {
          val n            = 100
          val maxBatchSize = 10
          val webhook      = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          val expectedRequestsMade = n / maxBatchSize

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Requests
          ) { requests =>
            assertM(requests.takeBetween(expectedRequestsMade, expectedRequestsMade + 1))(
              hasSize(equalTo(expectedRequestsMade))
            )
          }
        },
        testM("batches for multiple webhooks") {
          val eventCount   = 100
          val webhookCount = 10
          val maxBatchSize = eventCount / webhookCount // 10
          val webhooks     = createWebhooks(webhookCount)(
            WebhookStatus.Enabled,
            WebhookDeliveryMode.BatchedAtMostOnce
          )
          val events       = webhooks.map(_.id).flatMap(webhook => createPlaintextEvents(maxBatchSize)(webhook))

          val expectedRequestsMade = maxBatchSize

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = webhooks,
            events = events,
            ScenarioInterest.Requests
          ) { requests =>
            assertM(requests.takeBetween(expectedRequestsMade, expectedRequestsMade + 1))(
              hasSize(equalTo(expectedRequestsMade))
            )
          }
        },
        testM("events dispatched by batch are marked delivered") {
          val eventCount = 100
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(eventCount)(webhook.id),
            ScenarioInterest.Events
          ) { events =>
            assertM(
              events.filterOutput(_.status == WebhookEventStatus.Delivered).takeBetween(eventCount, eventCount + 1)
            )(hasSize(equalTo(eventCount)))
          }
        },
        testM("doesn't batch before max wait time") {
          val n       = 5 // less than max batch size 10
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val events  = createPlaintextEvents(n)(webhook.id)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests,
            adjustDuration = Some(2.seconds)
          )(_.take *> assertCompletesM)
        } @@ timeout(50.millis) @@ failing,
        testM("batches on max wait time") {
          val n       = 5 // less than max batch size 10
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val events  = createPlaintextEvents(n)(webhook.id)

          val expectedRequests = 1

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          ) { requests =>
            assertM(requests.takeBetween(expectedRequests, expectedRequests + 1))(hasSize(equalTo(expectedRequests)))
          }
        },
        testM("batches events on webhook and content-type") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          val jsonEvents      = createJsonEvents(4)(webhook.id)
          val plaintextEvents = createPlaintextEvents(4)(webhook.id)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents ++ plaintextEvents,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          )(requests => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
        },
        testM("JSON event contents are batched into a JSON array") {
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val jsonEvents = createJsonEvents(10)(webhook.id)

          val expectedOutput = "[" + (0 until 10).map(i => s"""{"event":"payload$i"}""").mkString(",") + "]"

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents,
            ScenarioInterest.Requests
          )(requests => assertM(requests.take.map(_.content))(equalTo(expectedOutput)))
        },
        testM("batched plain text event contents are appended") {
          val webhook         = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val plaintextEvents = createPlaintextEvents(2)(webhook.id)

          val expectedOutput = "event payload 0event payload 1"

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = plaintextEvents,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          )(requests => assertM(requests.take.map(_.content))(equalTo(expectedOutput)))
        },
        testM("failed batched deliveries are retried") {
          val n       = 100
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtLeastOnce)
          val events  = (0L until n.toLong).map { i =>
            WebhookEvent(
              WebhookEventKey(WebhookEventId(i), WebhookId(0)),
              WebhookEventStatus.New,
              i.toString + "\n",
              Chunk(("Accept", "*/*"), ("Content-Type", "text/plain"))
            )
          }

          val expectedCount = 20

          for {
            queues     <- ZIO.collectAll(Chunk.fill(10)(Queue.bounded[Option[WebhookHttpResponse]](2)))
            _          <- ZIO.collectAll(queues.map(_.offerAll(List(None, Some(WebhookHttpResponse(200))))))
            testResult <- webhooksTestScenario(
                            stubResponses = request => {
                              val firstNum = request.content.takeWhile(_ != '\n').toInt
                              queues.lift(firstNum / 10)
                            },
                            webhooks = List(webhook),
                            events = events,
                            ScenarioInterest.Requests,
                            adjustDuration = None
                          ) {
                            requests =>
                              val pollNext         = requests.poll <* TestClock.adjust(1.second)
                              val schedule         =
                                Schedule.recurUntil[Option[WebhookHttpRequest]](_.isDefined) &&
                                  Schedule.spaced(10.millis)
                              val expectedRequests = List.fill(expectedCount)(pollNext.repeat(schedule).map(_._1))
                              ZIO
                                .collectAll(expectedRequests)
                                .map(_.collect { case Some(request) => request })
                                .provideSomeLayer[TestClock](Clock.live) *> assertCompletesM
                          }
          } yield testResult
        } @@ timeout(2.seconds) @@ flaky // TODO[low-prio]: fix test flakiness
      ).injectSome[TestEnvironment](specEnv, WebhookServerConfig.defaultWithBatching),
      suite("shutdown and recovery")(
        suite("on shutdown")(
          testM("handles no events when shut down right after starting") {
            val webhook   = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val testEvent = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "event payload",
              plaintextContentHeaders
            )

            TestWebhookEventRepo.getEvents.map(_.filterOutput(_.status == WebhookEventStatus.Delivering)).use {
              events =>
                for {
                  responses <- Queue.unbounded[Option[WebhookHttpResponse]]
                  server    <- WebhookServer.create
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Some(WebhookHttpResponse(200)), Some(WebhookHttpResponse(200))))
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

            TestWebhookEventRepo.getEvents.map(_.filterOutput(_.status == WebhookEventStatus.Delivering)).use {
              events =>
                for {
                  responses <- Queue.unbounded[Option[WebhookHttpResponse]]
                  server    <- WebhookServer.create
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Some(WebhookHttpResponse(200)), Some(WebhookHttpResponse(200))))
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
          testM("lets batches complete") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
            val events  = createPlaintextEvents(5)(WebhookId(0))

            TestWebhookHttpClient.requests.use {
              requests =>
                for {
                  responses <- Queue.unbounded[Option[WebhookHttpResponse]]
                  server    <- WebhookServer.create
                  _         <- server.start
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Some(WebhookHttpResponse(200)), Some(WebhookHttpResponse(200))))
                  _         <- TestWebhookRepo.createWebhook(webhook)
                  _         <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
                  _         <- server.shutdown
                  _         <- requests.take
                } yield assertCompletes
            }
          }
        ),
        testM("restarted server continues retries") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
          val event   = WebhookEvent(
            WebhookEventKey(WebhookEventId(0), WebhookId(0)),
            WebhookEventStatus.New,
            "event content",
            plaintextContentHeaders
          )

          TestWebhookHttpClient.requests.use {
            requests =>
              for {
                responses <- Queue.unbounded[Option[WebhookHttpResponse]]
                server    <- WebhookServer.create
                _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                _         <- responses.offerAll(List(None, None, Some(WebhookHttpResponse(200))))
                _         <- server.start
                _         <- TestWebhookRepo.createWebhook(webhook)
                _         <- TestWebhookEventRepo.createEvent(event)
                _         <- requests.takeN(2)
                _         <- server.shutdown
                _         <- server.start
                _         <- TestClock.adjust(10.millis) // base exponential
                _         <- requests.takeN(2)
              } yield assertCompletes
          }
        } @@ timeout(2.seconds) @@ failing @@ ignore // TODO: get this right
      ).injectSome[TestEnvironment](mockEnv, WebhookServerConfig.defaultWithBatching)
      // TODO: write webhook status change tests
      // ) @@ nonFlaky(10) @@ timeout(30.seconds) @@ timed
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
          TestWebhookEventRepo.getEvents
        case ScenarioInterest.Requests =>
          TestWebhookHttpClient.requests
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
    stubResponses: WebhookHttpRequest => Option[Queue[Option[WebhookHttpResponse]]],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A],
    adjustDuration: Option[Duration]
  )(
    assertion: Dequeue[A] => URIO[TestClock, TestResult]
  ): URIO[SpecEnv with TestClock with Has[WebhookServer] with Clock, TestResult] =
    ScenarioInterest.dequeueFor(scenarioInterest).map(assertion).flatMap(_.forkManaged).use { testFiber =>
      for {
        _          <- TestWebhookHttpClient.setResponse(stubResponses)
        _          <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook)
        _          <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
        _          <- adjustDuration.map(TestClock.adjust(_)).getOrElse(ZIO.unit)
        testResult <- testFiber.join
      } yield testResult
    }

  def webhooksTestScenario[A](
    stubResponses: UStream[Option[WebhookHttpResponse]],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A],
    adjustDuration: Option[Duration] = None
  )(
    assertion: Dequeue[A] => URIO[TestClock, TestResult]
  ): URIO[SpecEnv with TestClock with Has[WebhookServer] with Clock, TestResult] =
    ScenarioInterest.dequeueFor(scenarioInterest).map(assertion).flatMap(_.forkManaged).use { testFiber =>
      for {
        responseQueue <- Queue.bounded[Option[WebhookHttpResponse]](1)
        _             <- stubResponses.run(ZSink.fromQueue(responseQueue)).fork
        _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
        _             <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook)
        _             <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
        _             <- adjustDuration.map(TestClock.adjust(_)).getOrElse(ZIO.unit)
        testResult    <- testFiber.join
      } yield testResult
    }
}
