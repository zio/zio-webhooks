package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration._
import zio.magic._
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

            WebhooksTestScenario(
              stubResponses = List(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = List(event),
              requestsAssertion = queue => assertM(queue.take)(equalTo(expectedRequest))
            ).build
          },
          testM("webhook is marked Delivering, then Delivered for successful dispatch") {
            val webhook = singleWebhook(0, WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)

            val event = WebhookEvent(
              WebhookEventKey(WebhookEventId(0), webhook.id),
              WebhookEventStatus.New,
              "event payload",
              Chunk(("Accept", "*/*"))
            )

            val expectedStatuses = List(WebhookEventStatus.Delivering, WebhookEventStatus.Delivered)

            WebhooksTestScenario(
              stubResponses = List(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = List(event),
              eventsAssertion = queue => {
                val markEvents    = queue.filterOutput(!_.status.isNew).takeN(2)
                val eventStatuses = markEvents.map(_.map(_.status))
                assertM(eventStatuses)(hasSameElements(expectedStatuses))
              }
            ).build
          },
          testM("can dispatch single event to n webhooks") {
            val n                 = 100
            val webhooks          = createWebhooks(n)(WebhookStatus.Enabled, WebhookDeliveryMode.SingleAtMostOnce)
            val eventsToNWebhooks = webhooks.map(_.id).flatMap(createWebhookEvents(1))

            WebhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = webhooks,
              events = eventsToNWebhooks,
              requestsAssertion = queue => assertM(queue.takeN(n))(hasSize(equalTo(n)))
            ).build
          },
          testM("dispatches no events for disabled webhooks") {
            val n       = 100
            val webhook = singleWebhook(0, WebhookStatus.Disabled, WebhookDeliveryMode.SingleAtMostOnce)

            WebhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion = queue => assertM(queue.takeAll.map(_.size))(equalTo(0)),
              sleepDuration = Some(100.millis)
            ).build
          },
          testM("dispatches no events for unavailable webhooks") {
            val n       = 100
            val webhook =
              singleWebhook(0, WebhookStatus.Unavailable(Instant.EPOCH), WebhookDeliveryMode.SingleAtMostOnce)

            WebhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion = queue => assertM(queue.takeAll.map(_.size))(equalTo(0)),
              sleepDuration = Some(100.millis)
            ).build
          }
        ),
        suite("with batched dispatch")(
          testM("batches events by max batch size") {
            val n            = 100
            val maxBatchSize = 10
            val webhook      = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            val expectedRequestsMade = n / maxBatchSize

            WebhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion =
                queue => assertM(queue.takeBetween(expectedRequestsMade, n).map(_.size))(equalTo(expectedRequestsMade))
            ).build
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

            WebhooksTestScenario(
              stubResponses = List.fill(eventCount)(WebhookHttpResponse(200)),
              webhooks = webhooks,
              events = events,
              requestsAssertion = queue =>
                assertM(queue.takeBetween(expectedRequestsMade, eventCount).map(_.size))(equalTo(expectedRequestsMade)),
              sleepDuration = Some(100.millis)
            ).build
          },
          // TODO: Ask how batch contents/headers should be put together
          testM("doesn't batch before max wait time") {
            val n       = 5 // less than max batch size 10
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            val expectedRequestsMade = 0

            WebhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion = queue =>
                assertM(
                  queue.takeBetween(expectedRequestsMade, n).map(_.size)
                )(equalTo(expectedRequestsMade)),
              adjustDuration = Some(3.seconds)
            ).build
          },
          testM("batches on max wait time") {
            val n       = 5 // less than max batch size 10
            val webhook = singleWebhook(id = 0, WebhookStatus.Enabled, WebhookDeliveryMode.BatchedAtMostOnce)

            val expectedRequestsMade = 1

            WebhooksTestScenario(
              stubResponses = List.fill(n)(WebhookHttpResponse(200)),
              webhooks = List(webhook),
              events = createWebhookEvents(n)(webhook.id),
              requestsAssertion = queue =>
                assertM(
                  queue.takeBetween(expectedRequestsMade, n).map(_.size)
                )(equalTo(expectedRequestsMade)),
              adjustDuration = Some(5.seconds)
            ).build
          }
        )
        // TODO: test that after 7 days have passed since webhook event delivery failure, a webhook is set unavailable
      )
    ).injectSome[TestClock with Clock](testEnv, BatchingConfig.live(10, 5.seconds)) @@ timeout(5.seconds)
}

object WebhookServerSpecUtil {

  type BatchAndClock = Has[Option[BatchingConfig]] with Clock

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
    with Has[WebhookServer]

  def testEnv[E]: ZLayer[BatchAndClock, E, TestEnv] =
    ZLayer
      .fromSomeMagic[BatchAndClock, TestEnv](
        TestWebhookRepo.test,
        TestWebhookEventRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookHttpClient.test,
        WebhookServer.live
      )
      .orDie

  final case class WebhooksTestScenario(
    stubResponses: Iterable[WebhookHttpResponse],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    eventsAssertion: Dequeue[WebhookEvent] => UIO[TestResult] = _ => assertCompletesM,
    // TODO[low-prio]: this should be a Dequeue so we don't inadvertently write to the Queue in tests
    requestsAssertion: Queue[WebhookHttpRequest] => UIO[TestResult] = _ => assertCompletesM,
    adjustDuration: Option[Duration] = None,
    sleepDuration: Option[Duration] = None
  ) {
    def build: RIO[TestEnv with TestClock, TestResult] =
      for {
        responseQueue <- Queue.unbounded[WebhookHttpResponse]
        _             <- responseQueue.offerAll(stubResponses)
        _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
        eventsFiber   <- TestWebhookEventRepo.subscribeToEvents(eventsAssertion).fork
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
}
