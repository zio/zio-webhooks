package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.magic._
import zio.stream._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.TestClock
import zio.webhooks.WebhookServer.BatchingConfig
import zio.webhooks.WebhookServerSpecUtil._
import zio.webhooks.testkit._

import java.time.Instant

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      suite("on new event subscription")(
        suite("with single dispatch")(
          testM("dispatches correct request given event") {
            val webhook = singleWebhook(0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload",
              Chunk(("Accept", "*/*"))
            )

            val expectedRequest = WebhookHttpRequest(webhook.url, event.content, event.headers)

            webhooksTestScenario(
              stubResponses = List(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = List(event),
              requestsAssertion = queue => assertM(queue.take)(equalTo(expectedRequest))
            )
          },
          testM("event is marked Delivering, then Delivered on successful dispatch") {
            val webhook = singleWebhook(0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload",
              Chunk(("Accept", "*/*"))
            )

            val expectedStatuses = List(WebhookEventStatus.Delivering, WebhookEventStatus.Delivered)

            webhooksTestScenario(
              stubResponses = List(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = List(event),
              eventsAssertion = events => {
                val markEvents    = events.filter(!_.status.isNew).take(2).runCollect
                val eventStatuses = markEvents.map(_.map(_.status))
                assertM(eventStatuses)(hasSameElements(expectedStatuses))
              }
            )
          },
          testM("can dispatch single event to n webhooks") {
            val n                 = 100
            val webhooks          = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
            val eventsToNWebhooks = webhooks.map(_.id).flatMap(createWebhookEvents(1))

            webhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = webhooks,
              events = eventsToNWebhooks,
              requestsAssertion = queue => assertM(queue.takeN(n))(hasSize(equalTo(n)))
            )
          },
          testM("dispatches no events for disabled webhooks") {
            val n       = 100
            val webhook = singleWebhook(0, WebhookStatus.Disabled, WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion = queue => assertM(queue.takeAll.map(_.size))(equalTo(0)),
              sleepDuration = Some(100.millis)
            )
          },
          testM("dispatches no events for unavailable webhooks") {
            val n       = 100
            val webhook =
              singleWebhook(0, WebhookStatus.Unavailable(Instant.EPOCH), WebhookDeliveryMode.SingleAtMostOnce)

            webhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion = queue => assertM(queue.takeAll.map(_.size))(equalTo(0)),
              sleepDuration = Some(100.millis)
            )
          }
        ),
        suite("with batched dispatch")(
          testM("batches events by max batch size") {
            val n            = 100
            val maxBatchSize = 10
            val webhook      = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            val expectedRequestsMade = n / maxBatchSize

            webhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion =
                queue => assertM(queue.takeBetween(expectedRequestsMade, n).map(_.size))(equalTo(expectedRequestsMade))
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
            val events       = webhooks.map(_.id).flatMap(createWebhookEvents(maxBatchSize))

            val expectedRequestsMade = maxBatchSize

            webhooksTestScenario(
              stubResponses = List.fill(eventCount)(WebhookHttpResponse(200)),
              webhooks = webhooks,
              events = events,
              requestsAssertion = queue =>
                assertM(
                  queue.takeBetween(expectedRequestsMade, eventCount).map(_.size)
                )(equalTo(expectedRequestsMade)),
              sleepDuration = Some(100.millis)
            )
          },
          testM("events dispatched by batch are marked delivered") {
            val eventCount = 100
            val webhook    = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            webhooksTestScenario(
              stubResponses = List.fill(10)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(eventCount)(webhook.id),
              eventsAssertion = events => {
                events.filter(_.status == WebhookEventStatus.Delivered).take(100).runCollect *> assertCompletesM
              }
            )
          },
          // TODO: Ask how batch contents/headers should be put together
          testM("doesn't batch before max wait time") {
            val n       = 5 // less than max batch size 10
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
            val events  = createWebhookEvents(n)(webhook.id)

            val expectedRequestsMade = 0

            webhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = events,
              requestsAssertion = queue =>
                assertM(
                  queue.takeBetween(expectedRequestsMade, n).map(_.size)
                )(equalTo(expectedRequestsMade)),
              adjustDuration = Some(2.seconds)
            )
          },
          testM("batches on max wait time") {
            val n       = 5 // less than max batch size 10
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)
            val events  = createWebhookEvents(n)(webhook.id)

            val expectedRequestsMade = 1

            webhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = events,
              requestsAssertion = queue =>
                assertM(
                  queue.takeBetween(expectedRequestsMade, n).map(_.size)
                )(equalTo(expectedRequestsMade)),
              adjustDuration = Some(5.seconds)
            )
          }
        )
        // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
      )
    ).injectSome[Has[Annotations.Service] with TestClock with Clock](testEnv) @@ timeout(5.seconds)
}

object WebhookServerSpecUtil {

  def createWebhooks(n: Int)(status: WebhookStatus, deliveryMode: WebhookDeliveryMode): Iterable[Webhook] =
    (0 until n).map(i => singleWebhook(i.toLong, status, deliveryMode))

  def createWebhookEvents(n: Int)(webhookId: WebhookId): Iterable[WebhookEvent] =
    (0 until n).map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i.toLong), webhookId),
        WebhookEventStatus.New,
        "event payload " + i,
        Chunk(("Accept", "*/*"))
      )
    }

  type EventsAssertion =
    ZStream[Has[TestWebhookEventRepo], Nothing, WebhookEvent] => URIO[Has[TestWebhookEventRepo], TestResult]

  def singleWebhook(id: Long, status: WebhookStatus, deliveryMode: WebhookDeliveryMode): Webhook =
    Webhook(
      WebhookId(id),
      "http://example.org/" + id,
      "testWebhook" + id,
      status,
      deliveryMode
    )

  type TestEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]
    with Has[Option[BatchingConfig]]
    with Has[WebhookServer]

  val testEnv: URLayer[Clock, TestEnv] =
    ZLayer
      .fromSomeMagic[Clock, TestEnv](
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookHttpClient.test,
        BatchingConfig.live(maxSize = 10, maxWaitTime = 5.seconds),
        WebhookServer.live
      )
      .orDie

  def webhooksTestScenario(
    stubResponses: Iterable[WebhookHttpResponse],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    eventsAssertion: EventsAssertion = _ => assertCompletesM,
    // TODO[low-prio]: this should be a Dequeue so we don't inadvertently write to the Queue in tests
    requestsAssertion: Queue[WebhookHttpRequest] => UIO[TestResult] = _ => assertCompletesM,
    adjustDuration: Option[Duration] = None,
    sleepDuration: Option[Duration] = None
  ): RIO[TestEnv with TestClock, TestResult] =
    for {
      responseQueue <- Queue.unbounded[WebhookHttpResponse]
      _             <- responseQueue.offerAll(stubResponses)
      _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
      eventsFiber   <- eventsAssertion(TestWebhookEventRepo.subscribeToEvents).fork
      _             <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook(_))
      _             <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent(_))
      requestQueue  <- TestWebhookHttpClient.requests
      // let test fiber sleep as we have to let requests be made to fail some tests
      // TODO[low-prio]: there's a better way to do this: poll the queue repeatedly with a timeout
      // TODO: see https://github.com/zio/zio/blob/31d9eacbb400c668460735a8a44fb68af9e5c311/core-tests/shared/src/test/scala/zio/ZQueueSpec.scala#L862
      _             <- adjustDuration.map(TestClock.adjust(_)).getOrElse(ZIO.unit)
      _             <- sleepDuration.map(Clock.Service.live.sleep(_)).getOrElse(ZIO.unit)
      eventsTest    <- eventsFiber.join
      requestsTest  <- requestsAssertion(requestQueue)
    } yield eventsTest && requestsTest
}
