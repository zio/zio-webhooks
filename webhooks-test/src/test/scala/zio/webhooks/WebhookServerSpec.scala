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
            stubResponses = List(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = List(event),
            ScenarioInterest.Requests
          )(requests => assertM(requests.runHead)(isSome(equalTo(expectedRequest))))
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
            stubResponses = List(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = List(event),
            ScenarioInterest.Events
          ) { events =>
            val eventStatuses = events.filter(!_.status.isNew).take(2).map(_.status).runCollect
            assertM(eventStatuses)(hasSameElements(expectedStatuses))
          }
        },
        testM("can dispatch single event to n webhooks") {
          val n                 = 100
          val webhooks          = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
          val eventsToNWebhooks = webhooks.map(_.id).flatMap(webhook => createPlaintextEvents(1)(webhook))

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = webhooks,
            events = eventsToNWebhooks,
            ScenarioInterest.Requests
          )(_.take(n.toLong).runDrain *> assertCompletesM)
        },
        testM("dispatches no events for disabled webhooks") {
          val n       = 100
          val webhook = singleWebhook(0, WebhookStatus.Disabled, WebhookDeliveryMode.SingleAtMostOnce)

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Requests
          )(requests => assertM(requests.runHead.timeout(100.millis).provideSomeLayer[SpecEnv](Clock.live))(isNone))
        },
        testM("dispatches no events for unavailable webhooks") {
          val n       = 100
          val webhook =
            singleWebhook(0, WebhookStatus.Unavailable(Instant.EPOCH), WebhookDeliveryMode.SingleAtMostOnce)

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Requests
          )(requests => assertM(requests.runHead.timeout(100.millis).provideSomeLayer[SpecEnv](Clock.live))(isNone))
        },
        testM("doesn't batch with disabled batching config ") {
          val n       = 100
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Requests
          )(_.take(n.toLong).runDrain *> assertCompletesM)
        },
        testM("an at-most-once webhook returning non-200 fails events") {
          val n       = 100
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(404))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Events
          )(
            _.collect { case ev if ev.status == WebhookEventStatus.Failed => ev.status }
              .take(100)
              .runDrain *> assertCompletesM
          )
        },
        testM("missing webhook errors are sent to stream") {
          val idRange               = 401L to 404L
          val missingWebhookIds     = idRange.map(WebhookId(_))
          val eventsMissingWebhooks = missingWebhookIds.flatMap(id => createPlaintextEvents(1)(id))

          val expectedErrorCount = missingWebhookIds.size

          webhooksTestScenario(
            stubResponses = List(Some(WebhookHttpResponse(200))),
            webhooks = List.empty,
            events = eventsMissingWebhooks,
            ScenarioInterest.Errors
          )(errors =>
            assertM(errors.take(expectedErrorCount.toLong).runCollect)(
              hasSameElements(idRange.map(id => MissingWebhookError(WebhookId(id))))
            )
          )
        },
        testM("immediately retries once on non-200 response") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

          val events = createPlaintextEvents(1)(webhook.id)

          webhooksTestScenario(
            stubResponses = List(Some(WebhookHttpResponse(500)), Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests
          )(_.take(2).runDrain *> assertCompletesM)
        },
        testM("immediately retries once on IOException") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

          val events = createPlaintextEvents(1)(webhook.id)

          webhooksTestScenario(
            stubResponses = List(None, Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests
          )(_.take(2).runDrain *> assertCompletesM)
        },
        testM("retrying sets webhook status to Retrying, then Enabled on success") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtLeastOnce)

          val events = createPlaintextEvents(1)(webhook.id)

          webhooksTestScenario(
            stubResponses = List(None, Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Webhooks
          )(webhooks =>
            assertM(webhooks.drop(1).take(2).runCollect)(
              hasSameElements(List(WebhookStatus.Enabled, WebhookStatus.Retrying(Instant.EPOCH), WebhookStatus.Enabled))
            )
          )
        } @@ timeout(128.millis) @@ failing
      ).injectSome[TestEnvironment](specEnv, BatchingConfig.disabled),
      suite("batching enabled")(
        testM("batches events by max batch size") {
          val n            = 100
          val maxBatchSize = 10
          val webhook      = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          val expectedRequestsMade = n / maxBatchSize

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(n)(webhook.id),
            ScenarioInterest.Requests
          )(_.take(expectedRequestsMade.toLong).runDrain *> assertCompletesM)
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
            stubResponses = List.fill(eventCount)(Some(WebhookHttpResponse(200))),
            webhooks = webhooks,
            events = events,
            ScenarioInterest.Requests
          )(_.take(expectedRequestsMade.toLong).runDrain *> assertCompletesM)
        },
        testM("events dispatched by batch are marked delivered") {
          val eventCount = 100
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          webhooksTestScenario(
            stubResponses = List.fill(10)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = createPlaintextEvents(eventCount)(webhook.id),
            ScenarioInterest.Events
          )(_.filter(_.status == WebhookEventStatus.Delivered).take(eventCount.toLong).runDrain *> assertCompletesM)
        },
        testM("doesn't batch before max wait time") {
          val n       = 5 // less than max batch size 10
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val events  = createPlaintextEvents(n)(webhook.id)

          val expectedRequestsMade = 0

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests,
            adjustDuration = Some(2.seconds)
          )(_.take(expectedRequestsMade.toLong).runDrain *> assertCompletesM)
        },
        testM("batches on max wait time") {
          val n       = 5 // less than max batch size 10
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val events  = createPlaintextEvents(n)(webhook.id)

          val expectedRequestsMade = 1

          webhooksTestScenario(
            stubResponses = List.fill(n)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = events,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          )(_.take(expectedRequestsMade.toLong).runDrain *> assertCompletesM)
        },
        testM("batches events on webhook and content-type") {
          val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

          val jsonEvents      = createJsonEvents(4)(webhook.id)
          val plaintextEvents = createPlaintextEvents(4)(webhook.id)

          webhooksTestScenario(
            stubResponses = List.fill(2)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents ++ plaintextEvents,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          )(_.take(2).runDrain *> assertCompletesM)
        },
        testM("JSON event contents are batched into a JSON array") {
          val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val jsonEvents = createJsonEvents(2)(webhook.id)

          val expectedOutput = """[{"event":"payload0"},{"event":"payload1"}]"""

          webhooksTestScenario(
            stubResponses = List.fill(2)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = jsonEvents,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          )(requests => assertM(requests.runHead.map(_.map(_.content)))(isSome(equalTo(expectedOutput))))
        },
        testM("batched plain text event contents are appended") {
          val webhook         = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
          val plaintextEvents = createPlaintextEvents(2)(webhook.id)

          val expectedOutput = "event payload 0event payload 1"

          webhooksTestScenario(
            stubResponses = List.fill(2)(Some(WebhookHttpResponse(200))),
            webhooks = List(webhook),
            events = plaintextEvents,
            ScenarioInterest.Requests,
            adjustDuration = Some(5.seconds)
          )(requests => assertM(requests.runHead.map(_.map(_.content)))(isSome(equalTo(expectedOutput))))
        }
      ).injectSome[TestEnvironment](specEnv, BatchingConfig.default)
      // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
      // ) @@ timeout(5.seconds)
    ) @@ nonFlaky
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

  val jsonContentHeaders = Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))

  sealed trait ScenarioInterest[A]
  object ScenarioInterest {
    case object Errors   extends ScenarioInterest[WebhookError]
    case object Events   extends ScenarioInterest[WebhookEvent]
    case object Requests extends ScenarioInterest[WebhookHttpRequest]
    case object Webhooks extends ScenarioInterest[Webhook]

    final def streamFor[A](scenarioInterest: ScenarioInterest[A]): URManaged[SpecEnv, UStream[A]] =
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
    stubResponses: Iterable[Option[WebhookHttpResponse]],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    scenarioInterest: ScenarioInterest[A],
    adjustDuration: Option[Duration] = None
  )(
    assertion: ZStream[SpecEnv, Nothing, A] => URIO[SpecEnv, TestResult]
  ): URIO[SpecEnv with TestClock with Has[WebhookServer] with Clock, TestResult] =
    (ScenarioInterest.streamFor(scenarioInterest).map(assertion).flatMap(_.forkManaged)).use { testFiber =>
      for {
        responseQueue <- Queue.unbounded[Option[WebhookHttpResponse]]
        _             <- responseQueue.offerAll(stubResponses)
        _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
        _             <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook(_))
        _             <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent(_))
        _             <- adjustDuration.map(TestClock.adjust(_)).getOrElse(ZIO.unit)
        testResult    <- testFiber.join
      } yield testResult
    }
}
