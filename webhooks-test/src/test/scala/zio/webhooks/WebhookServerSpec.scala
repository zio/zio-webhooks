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
import zio.webhooks.WebhookServer.BatchingConfig
import zio.webhooks.WebhookServerSpecUtil._
import zio.webhooks.WebhookStatus._
import zio.webhooks.testkit._

import java.time.Instant

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      suite("batching disabled")(
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
        testM("doesn't batch with disabled batching config ") {
          val n       = 100
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Requests
          )(requests => assertM(requests.takeBetween(n, n + 1))(hasSize(equalTo(n))))
        },
        testM("an at-most-once webhook returning non-200 fails events") {
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
        testM("missing webhook errors are sent to stream") {
          val idRange               = 401L to 404L
          val missingWebhookIds     = idRange.map(WebhookId)
          val eventsMissingWebhooks = missingWebhookIds.flatMap(id => createPlaintextEvents(1)(id))

          val expectedErrorCount = missingWebhookIds.size

          webhooksTestScenario(
            stubResponses = UStream(Some(WebhookHttpResponse(200))),
            webhooks = List.empty,
            events = eventsMissingWebhooks,
            ScenarioInterest.Errors
          )(errors =>
            assertM(errors.takeBetween(expectedErrorCount, expectedErrorCount + 1))(
              hasSameElements(idRange.map(id => MissingWebhookError(WebhookId(id))))
            )
          )
        },
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
          )(webhooks =>
            assertM(webhooks.takeN(3).map(_.drop(1).map(_.status)))(
              hasSameElements(List(WebhookStatus.Retrying(Instant.EPOCH), WebhookStatus.Enabled))
            )
          )
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
          )(webhooks =>
            for {
              status2 <- webhooks.take *> webhooks.take.map(_.status)
              _       <- TestClock.adjust(timeElapsed)
              status3 <- webhooks.take.map(_.status)
            } yield assertTrue(status2 == Retrying(Instant.EPOCH)) &&
              assertTrue(status3 == Unavailable(Instant.EPOCH.plus(timeElapsed)))
          )
        },
        testM("retries past first one backs off exponentially") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)
          val events  = createPlaintextEvents(1)(webhook.id)

          webhooksTestScenario(
            stubResponses = UStream.repeat(None).take(6) ++ UStream(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests
          )(requests =>
            for {
              request1 <- requests.take.as(true)
              request2 <- requests.take.as(true)
              _        <- TestClock.adjust(10.millis)
              request3 <- requests.take.as(true)
              _        <- TestClock.adjust(10.millis)
              request4 <- requests.poll
              _        <- TestClock.adjust(10.millis)
              request5 <- requests.take.as(true)
            } yield assertTrue(request1 && request2 && request3 && request5) &&
              assertTrue(request4.isEmpty)
          )
        }
        // TODO: test that retries past first one back off exponentially
        // TODO: test that it retries for multiple webhooks
      ).injectSome[TestEnvironment](specEnv, BatchingConfig.disabled),
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
          )(requests =>
            assertM(requests.takeBetween(expectedRequestsMade, expectedRequestsMade + 1))(
              hasSize(equalTo(expectedRequestsMade))
            )
          )
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
          )(requests =>
            assertM(requests.takeBetween(expectedRequestsMade, expectedRequestsMade + 1))(
              hasSize(equalTo(expectedRequestsMade))
            )
          )
        },
        testM("events dispatched by batch are marked delivered") {
          val eventCount = 100
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          webhooksTestScenario(
            stubResponses = UStream.repeat(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(eventCount)(webhook.id),
            ScenarioInterest.Events
          )(events =>
            assertM(
              events.filterOutput(_.status == WebhookEventStatus.Delivered).takeBetween(eventCount, eventCount + 1)
            )(hasSize(equalTo(eventCount)))
          )
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
          )(requests =>
            assertM(requests.takeBetween(expectedRequests, expectedRequests + 1))(hasSize(equalTo(expectedRequests)))
          )
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
        }
      ).injectSome[TestEnvironment](specEnv, BatchingConfig.default)
      // TODO: write webhook status change tests
      //    ) @@ nonFlaky @@ timeout(2.minutes)
    ) @@ timeout(10.seconds)
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
        Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
      )
    }

  def createPlaintextEvents(n: Int)(webhookId: WebhookId): Iterable[WebhookEvent] =
    (0 until n).map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i.toLong), webhookId),
        WebhookEventStatus.New,
        "event payload " + i,
        Chunk(("Accept", "*/*"), ("Content-Type", "text/plain"))
      )
    }

  val jsonContentHeaders: Chunk[(String, String)] = Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))

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

  lazy val specEnv: URLayer[Clock with Has[Option[BatchingConfig]], SpecEnv] =
    ZLayer
      .fromSomeMagic[Clock with Has[Option[BatchingConfig]], SpecEnv](
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

  def webhooksTestScenario[A](
    stubResponses: UStream[Option[WebhookHttpResponse]],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A],
    adjustDuration: Option[Duration] = None
  )(
    assertion: Dequeue[A] => URIO[TestClock, TestResult]
  ): URIO[SpecEnv with TestClock with Has[WebhookServer] with Clock, TestResult] =
    ScenarioInterest
      .dequeueFor(scenarioInterest)
      .map(assertion)
      .flatMap(_.forkManaged)
      .use { testFiber =>
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
