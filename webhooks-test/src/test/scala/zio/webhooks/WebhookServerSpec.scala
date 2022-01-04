package zio.webhooks

import zio._
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.json._
import zio.magic._
import zio.stream._
import zio.test.Assertion._
import zio.test.TestAspect.{ failing, timeout }
import zio.test._
import zio.test.environment._
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookServerSpecUtil._
import zio.webhooks.WebhookUpdate.WebhookChanged
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.internal.PersistentRetries
import zio.webhooks.testkit.TestWebhookHttpClient._
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
              initialStubResponses = UStream(Right(WebhookHttpResponse(200))),
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
              initialStubResponses = UStream(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Webhooks
            )((webhooks, _) => assertM(webhooks.take)(equalTo(WebhookChanged(webhook))))
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
              initialStubResponses = UStream(Right(WebhookHttpResponse(200))),
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
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = webhooks,
              events = eventsToNWebhooks,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
          },
          testM("dispatches no events for disabled webhooks") {
            val n       = 100
            val webhook = singleWebhook(0, WebhookStatus.Disabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
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
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )((requests, _) => requests.take *> assertCompletesM)
          } @@ timeout(50.millis) @@ failing,
          testM("doesn't batch when no batching configuration is given") {
            val n       = 100
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
          },
          testM("a webhook receiver returning non-200 fails events") {
            val n       = 100
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(404))),
              webhooks = List(webhook),
              events = createPlaintextEvents(n)(webhook.id),
              ScenarioInterest.Events
            ) { (events, _) =>
              assertM(
                events.map(_.status).filterOutput(_ == WebhookEventStatus.Failed).takeBetween(n, n + 1)
              )(hasSize(equalTo(n)))
            }
          },
          testM("server dies with a fatal error when webhooks are missing") {
            val idRange               = 401L to 404L
            val missingWebhookIds     = idRange.map(WebhookId(_))
            val eventsMissingWebhooks = missingWebhookIds.flatMap(id => createPlaintextEvents(1)(id))

            webhooksTestScenario(
              initialStubResponses = UStream(Right(WebhookHttpResponse(200))),
              webhooks = List.empty,
              events = eventsMissingWebhooks,
              ScenarioInterest.Errors
            )((errors, _) => assertM(errors.take)(isSubtype[FatalError](anything)))
          },
          testM("bad webhook URL errors are published") {
            val webhookWithBadUrl = Webhook(
              id = WebhookId(0),
              url = "ne'er-do-well URL",
              label = "webhook with a bad url",
              WebhookStatus.Enabled,
              WebhookDeliveryMode.SingleAtMostOnce
            )

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "test event payload",
              plaintextContentHeaders
            )

            val expectedError = BadWebhookUrlError(webhookWithBadUrl.url, "'twas a ne'er do-well")

            webhooksTestScenario(
              initialStubResponses = UStream(Left(Some(expectedError))),
              webhooks = List(webhookWithBadUrl),
              events = List(event),
              ScenarioInterest.Errors
            ) { (errors, _) =>
              assertM(errors.take)(equalTo(expectedError))
            }
          },
          testM("do not retry events") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), WebhookId(0)),
              WebhookEventStatus.New,
              "test event payload",
              plaintextContentHeaders
            )

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Left(None)),
              webhooks = List(webhook),
              events = List(event),
              ScenarioInterest.Requests
            ) { (requests, _) =>
              for {
                _             <- requests.take
                secondRequest <- requests.take.timeout(100.millis).provideLayer(Clock.live)
              } yield assert(secondRequest)(isNone)
            }
          },
          testM("writes warning to console when delivering to a slow webhook, i.e. queue gets full") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            for {
              capacity   <- ZIO.service[WebhookServerConfig].map(_.webhookQueueCapacity)
              clock      <- ZIO.environment[Clock]
              testEvents  = createPlaintextEvents(capacity + 2)(webhook.id) // + 2 because the first one gets taken
              testResult <- webhooksTestScenario(
                              initialStubResponses =
                                UStream.fromEffect(UIO.right(WebhookHttpResponse(200)).delay(1.minute)).provide(clock),
                              webhooks = List(webhook),
                              events = List.empty,
                              ScenarioInterest.Events
                            ) { (_, _) =>
                              for {
                                _ <- ZIO.foreach_(testEvents)(TestWebhookEventRepo.createEvent)
                                _ <- TestConsole.output.repeatUntil(_.nonEmpty)
                              } yield assertCompletes
                            }
            } yield testResult
          }
        ),
        suite("webhooks with at-least-once delivery")(
          testM("immediately retries once on non-200 response") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              initialStubResponses = UStream(Right(WebhookHttpResponse(500)), Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
          },
          testM("immediately retries once on IOException") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              initialStubResponses = UStream(Left(None), Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            )((requests, _) => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
          },
          testM("retries until success before retry timeout") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              initialStubResponses = UStream(Left(None), Left(None), Right(WebhookHttpResponse(200))),
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
          testM("webhook is set unavailable after retry timeout") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(1)(webhook.id)

            webhooksTestScenario(
              initialStubResponses = UStream(Left(None), Left(None)),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Webhooks
            ) { (webhooks, _) =>
              for {
                status  <- webhooks.take.map(_.status)
                status2 <- (webhooks.take.map(_.status) race TestClock.adjust(7.days).forever)
              } yield assert(status)(isSome(equalTo(WebhookStatus.Enabled))) &&
                assert(status2)(isSome(isSubtype[WebhookStatus.Unavailable](Assertion.anything)))
            }
          },
          testM("marks all a webhook's events failed when marked unavailable") {
            val n       = 2
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
            val events  = createPlaintextEvents(n)(webhook.id)

            webhooksTestScenario(
              initialStubResponses = UStream(Left(None), Left(None)),
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
              initialStubResponses =
                UStream.fromIterable(List.fill(5)(Left(None))) ++ UStream(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = events,
              ScenarioInterest.Requests
            ) {
              (requests, _) =>
                for {
                  _    <- requests.take // 1st failure
                  _    <- requests.take // 1st retry immediately after
                  _    <- TestClock.adjust(10.millis)
                  _    <- requests.take // 2nd retry after 10ms
                  _    <- TestClock.adjust(20.millis)
                  _    <- requests.take // 3rd retry after 20ms
                  _    <- TestClock.adjust(10.millis)
                  poll <- requests.poll
                  _    <- TestClock.adjust(30.millis)
                  _    <- requests.take // 4th retry after 40ms
                  _    <- TestClock.adjust(80.millis)
                  _    <- requests.take // 5th retry after 80ms
                } yield assert(poll)(isNone)
            }
          },
          testM("doesn't retry requests after requests succeed again") {
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

            val events = createPlaintextEvents(3)(webhook.id)

            webhooksTestScenario(
              initialStubResponses = UStream(Left(None)) ++ UStream.repeat(Right(WebhookHttpResponse(200))),
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
        ),
        suite("on webhook changes")(
          testM("changing a webhook's URL eventually changes the request URL") {
            val firstUrl  = "first url"
            val secondUrl = "second url"

            val webhook =
              Webhook(
                WebhookId(0),
                firstUrl,
                "test webhook",
                WebhookStatus.Enabled,
                WebhookDeliveryMode.SingleAtMostOnce
              )

            val firstEvent = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload 0",
              plaintextContentHeaders
            )

            val nextEvents = UStream
              .iterate(0L)(_ + 1)
              .map { eventId =>
                WebhookEvent(
                  WebhookEventKey(WebhookEventId(eventId), webhook.id),
                  WebhookEventStatus.New,
                  s"event payload $eventId",
                  plaintextContentHeaders
                )
              }
              .drop(1)
              .schedule(Schedule.spaced(1.milli))
              .provideLayer(Clock.live)

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List.empty,
              ScenarioInterest.Requests
            ) { (requests, _) =>
              for {
                _              <- TestWebhookEventRepo.createEvent(firstEvent)
                actualFirstUrl <- requests.take.map(_.url)
                _              <- TestWebhookRepo.setWebhook(webhook.copy(url = secondUrl))
                _              <- nextEvents.foreach(TestWebhookEventRepo.createEvent).fork
                _              <- requests.filterOutput(_.url == secondUrl).take
              } yield assertTrue(actualFirstUrl == firstUrl)
            }
          },
          testM("toggling a webhook's status toggles event delivery") {
            val webhook =
              Webhook(
                WebhookId(0),
                "test url",
                "test webhook",
                WebhookStatus.Enabled,
                WebhookDeliveryMode.SingleAtMostOnce
              )

            val firstEvent = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload 0",
              plaintextContentHeaders
            )

            val nextEvents = UStream
              .iterate(0L)(_ + 1)
              .map { eventId =>
                WebhookEvent(
                  WebhookEventKey(WebhookEventId(eventId), webhook.id),
                  WebhookEventStatus.New,
                  s"event payload $eventId",
                  plaintextContentHeaders
                )
              }
              .drop(1)
              .schedule(Schedule.spaced(1.milli))
              .provideLayer(Clock.live)

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
              webhooks = List(webhook),
              events = List.empty,
              ScenarioInterest.Events
            ) {
              (events, _) =>
                for {
                  _               <- TestWebhookEventRepo.createEvent(firstEvent)
                  deliveringEvents = events.filterOutput(_.isDelivering)
                  _               <- deliveringEvents.take
                  _               <- TestWebhookRepo.setWebhook(webhook.disable)
                  _               <- nextEvents.foreach(TestWebhookEventRepo.createEvent).fork
                  _               <- deliveringEvents.take
                                       .timeout(2.millis)
                                       .repeatUntil(_.isEmpty)
                                       .provideLayer(Clock.live)
                  _               <- TestWebhookRepo.setWebhook(webhook.enable)
                  _               <- deliveringEvents.take
                                       .timeout(2.millis)
                                       .repeatUntil(_.isDefined)
                                       .provideLayer(Clock.live)
                } yield assertCompletes
            }
          },
          testM("setting a webhook's delivery semantics to at-least-once enables retries") {
            val webhook =
              Webhook(
                WebhookId(0),
                "test url",
                "test webhook",
                WebhookStatus.Enabled,
                WebhookDeliveryMode.SingleAtMostOnce
              )

            val firstEvent = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload 0",
              plaintextContentHeaders
            )

            val nextEvents = UStream
              .iterate(0L)(_ + 1)
              .map { eventId =>
                WebhookEvent(
                  WebhookEventKey(WebhookEventId(eventId), webhook.id),
                  WebhookEventStatus.New,
                  s"event payload $eventId",
                  plaintextContentHeaders
                )
              }
              .drop(1)
              .schedule(Schedule.spaced(1.milli))
              .provideLayer(Clock.live)

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Left(None)),
              webhooks = List(webhook),
              events = List.empty,
              ScenarioInterest.Requests
            ) {
              (requests, _) =>
                for {
                  _   <- TestWebhookEventRepo.createEvent(firstEvent)
                  ref <- Ref.make(Set.empty[WebhookHttpRequest])
                  _   <- requests.take.flatMap(req => ref.modify(attempted => (attempted(req), attempted + req)))
                  _   <- TestWebhookRepo.setWebhook(
                           webhook.copy(deliveryMode = WebhookDeliveryMode.SingleAtLeastOnce)
                         )
                  _   <- nextEvents.foreach(TestWebhookEventRepo.createEvent).fork
                  _   <- requests.take
                           .flatMap(req => ref.modify(attempted => (attempted(req), attempted + req)))
                           .repeatUntil(identity)
                  _   <- ref.set(Set.empty)
                } yield assertCompletes
            }
          },
          testM("disabling a webhook with at-least-once delivery semantics halts retries") {
            val webhook =
              Webhook(
                WebhookId(0),
                "test url",
                "test webhook",
                WebhookStatus.Enabled,
                WebhookDeliveryMode.SingleAtLeastOnce
              )

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload 0",
              plaintextContentHeaders
            )

            webhooksTestScenario(
              initialStubResponses = UStream.repeat(Left(None)),
              webhooks = List(webhook),
              events = List.empty,
              ScenarioInterest.Requests
            ) { (requests, _) =>
              for {
                _          <- TestWebhookEventRepo.createEvent(event)
                _          <- requests.take
                _          <- TestWebhookRepo.setWebhook(webhook.copy(status = WebhookStatus.Disabled))
                waitForHalt = requests.take.timeout(50.millis).repeatUntil(_.isEmpty).provideLayer(Clock.live)
                _          <- waitForHalt race TestClock.adjust(10.millis).forever
              } yield assertCompletes
            }
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
            initialStubResponses = UStream.empty,
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
            initialStubResponses = UStream.empty,
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
            initialStubResponses = UStream.empty,
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

          val jsonEvents =
            (0 until 4).map { i =>
              WebhookEvent(
                WebhookEventKey(WebhookEventId(i.toLong), webhook.id),
                WebhookEventStatus.New,
                s"""{"event":"payload$i"}""",
                jsonContentHeaders
              )
            }

          val plaintextEvents =
            (4 until 8).map { i =>
              WebhookEvent(
                WebhookEventKey(WebhookEventId(i.toLong), webhook.id),
                WebhookEventStatus.New,
                "event payload " + i,
                plaintextContentHeaders
              )
            }

          webhooksTestScenario(
            initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents ++ plaintextEvents,
            ScenarioInterest.Requests
          )((requests, _) => assertM(requests.takeBetween(2, 3))(hasSize(equalTo(2))))
        },
        testM("batched JSON event contents are always serialized into a JSON array") {
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val jsonEvents = createJsonEvents(100)(webhook.id)

          webhooksTestScenario(
            initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents,
            ScenarioInterest.Requests
          )((requests, _) => assertM(requests.take.map(_.content))(matchesRegex(jsonPayloadPattern)))
        },
        testM("batched plain text event contents are concatenated") {
          val n               = 2
          val webhook         = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val plaintextEvents = createPlaintextEvents(n)(webhook.id)

          webhooksTestScenario(
            initialStubResponses = UStream.repeat(Right(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = plaintextEvents,
            ScenarioInterest.Requests
          )((requests, _) => assertM(requests.take.map(_.content))(matchesRegex("(?:event payload \\d+)+")))
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
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(
                                 List(Right(WebhookHttpResponse(200)), Right(WebhookHttpResponse(200)))
                               )
                  _         <- WebhookServer.start.useNow
                  _         <- TestWebhookRepo.setWebhook(webhook)
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
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(
                                 List(Right(WebhookHttpResponse(200)), Right(WebhookHttpResponse(200)))
                               )
                  event1    <- WebhookServer.start.use_ {
                                 for {
                                   _      <- TestWebhookRepo.setWebhook(webhook)
                                   _      <- TestWebhookEventRepo.createEvent(testEvents(0))
                                   event1 <- events.take.as(true)
                                 } yield event1
                               }
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
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Left(None), Left(None)))
                  _         <- WebhookServer.start.use_ {
                                 for {
                                   _ <- TestWebhookRepo.setWebhook(webhook)
                                   _ <- TestWebhookEventRepo.createEvent(event)
                                   _ <- requests.takeN(2) // wait for 2 requests to come through
                                 } yield ()
                               }
                  saveState <- WebhookStateRepo.loadState
                                 .repeatUntil(_.isDefined)
                                 .map {
                                   _.map(_.fromJson[PersistentRetries])
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
              "recovered event",
              plaintextContentHeaders
            )

            TestWebhookHttpClient.getRequests.use {
              requests =>
                for {
                  responses <- Queue.unbounded[StubResponse]
                  _         <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _         <- responses.offerAll(List(Left(None), Left(None), Right(WebhookHttpResponse(200))))
                  _         <- WebhookServer.start.use_ {
                                 for {
                                   _ <- TestWebhookRepo.setWebhook(webhook)
                                   _ <- TestWebhookEventRepo.createEvent(event)
                                   _ <- requests.takeN(2)
                                 } yield ()
                               }
                  _         <- WebhookServer.start.use_(requests.take.fork)
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

            (TestWebhookHttpClient.getRequests zip TestWebhookRepo.subscribeToWebhooks).use {
              case (requests, webhooks) =>
                for {
                  responses  <- Queue.unbounded[StubResponse]
                  _          <- TestWebhookHttpClient.setResponse(_ => Some(responses))
                  _          <- responses.offerAll(List(Left(None), Left(None), Right(WebhookHttpResponse(200))))
                  _          <- WebhookServer.start.use_ {
                                  for {
                                    _ <- TestWebhookRepo.setWebhook(webhook)
                                    _ <- TestWebhookEventRepo.createEvent(event)
                                    _ <- requests.takeN(2)
                                    _ <- TestClock.adjust(3.days)
                                  } yield ()
                                }
                  lastStatus <- WebhookServer.start.use_ {
                                  for {
                                    _          <- TestClock.adjust(4.days)
                                    lastStatus <- webhooks.takeN(2).map(_.last.status)
                                    _          <- requests.take
                                  } yield lastStatus
                                }

                } yield assert(lastStatus)(isSome(isSubtype[WebhookStatus.Unavailable](anything)))
            }
          },
          testM("clears persisted state after loading") {
            for {
              _              <- WebhookServer.start.useNow
              persistedState <- ZIO.serviceWith[WebhookStateRepo](_.loadState)
              _              <- WebhookServer.start.use_(
                                  ZIO.serviceWith[WebhookStateRepo](_.loadState).repeatUntil(_.isEmpty)
                                )
            } yield assert(persistedState)(isSome(anything))
          }
          // TODO: write unit tests for persistent retry backoff when needed
        )
      ).injectSome[TestEnvironment](mockEnv, WebhookServerConfig.default)
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

  val jsonContentHeaders: Chunk[HttpHeader] = Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))

  val jsonPayloadPattern: String =
    """(?:\[\{\"event\":\"payload\d+\"}(?:,\{\"event\":\"payload\d+\"})*\])"""

  type MockEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]
    with Has[WebhooksProxy]
    with Has[SerializePayload]

  lazy val mockEnv: ZLayer[Clock with Has[WebhookServerConfig], Nothing, MockEnv] =
    ZLayer
      .fromSomeMagic[Clock with Has[WebhookServerConfig], MockEnv](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookHttpClient.test,
        TestWebhookRepo.subscriptionUpdateMode,
        TestWebhookRepo.test,
        WebhooksProxy.live
      )

  val plaintextContentHeaders: Chunk[HttpHeader] = Chunk(("Accept", "*/*"), ("Content-Type", "text/plain"))

  sealed trait ScenarioInterest[A]
  object ScenarioInterest {
    case object Errors   extends ScenarioInterest[WebhookError]
    case object Events   extends ScenarioInterest[WebhookEvent]
    case object Requests extends ScenarioInterest[WebhookHttpRequest]
    case object Webhooks extends ScenarioInterest[WebhookUpdate]

    final def dequeueFor[A](scenarioInterest: ScenarioInterest[A]): URManaged[SpecEnv, Dequeue[A]] =
      scenarioInterest match {
        case ScenarioInterest.Errors   =>
          ZManaged.service[WebhookServer].flatMap(_.subscribeToErrors)
        case ScenarioInterest.Events   =>
          TestWebhookEventRepo.subscribeToEvents
        case ScenarioInterest.Requests =>
          TestWebhookHttpClient.getRequests
        case ScenarioInterest.Webhooks =>
          TestWebhookRepo.subscribeToWebhooks
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
    with Has[WebhooksProxy]

  lazy val specEnv: URLayer[Clock with Console with Has[WebhookServerConfig], SpecEnv] =
    ZLayer
      .fromSomeMagic[Clock with Console with Has[WebhookServerConfig], SpecEnv](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookHttpClient.test,
        TestWebhookRepo.subscriptionUpdateMode,
        TestWebhookRepo.test,
        WebhookServer.live,
        WebhooksProxy.live
      )

  // keep an eye on the duplication here
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
        _          <- ZIO.foreach_(webhooks)(TestWebhookRepo.setWebhook)
        _          <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
        testResult <- testFiber.join
      } yield testResult
    }

  def webhooksTestScenario[A](
    initialStubResponses: UStream[StubResponse],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A]
  )(
    assertion: (Dequeue[A], Queue[StubResponse]) => URIO[SpecEnv with TestClock with TestConsole, TestResult]
  ): URIO[SpecEnv with TestClock with TestConsole with Has[WebhookServer] with Clock, TestResult] =
    ScenarioInterest.dequeueFor(scenarioInterest).use { dequeue =>
      for {
        responseQueue <- Queue.bounded[StubResponse](1)
        testFiber     <- assertion(dequeue, responseQueue).fork
        _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
        _             <- ZIO.foreach_(webhooks)(TestWebhookRepo.setWebhook)
        _             <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent)
        _             <- initialStubResponses.run(ZSink.fromQueue(responseQueue)).fork
        testResult    <- testFiber.join
      } yield testResult
    }
}
