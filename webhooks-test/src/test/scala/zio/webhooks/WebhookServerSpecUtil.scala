package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.test._
import zio.webhooks.testkit._

// TODO: scaladoc
object WebhookServerSpecUtil {

  def assertRequestsMade(
    stubResponses: Iterable[WebhookHttpResponse],
    webhooks: Iterable[Webhook],
    events: Iterable[WebhookEvent],
    requestsAssertion: Queue[WebhookHttpRequest] => UIO[TestResult],
    sleepDuration: Option[Duration] = None
  ): URIO[TestEnv, TestResult] =
    for {
      responseQueue <- Queue.unbounded[WebhookHttpResponse]
      _             <- responseQueue.offerAll(stubResponses)
      _             <- TestWebhookHttpClient.setResponse(_ => Some(responseQueue))
      _             <- ZIO.foreach_(webhooks)(TestWebhookRepo.createWebhook(_))
      _             <- ZIO.foreach_(events)(TestWebhookEventRepo.createEvent(_))
      requestQueue  <- TestWebhookHttpClient.requests
      // let test fiber sleep as we have to let requests be made to fail some tests
      // TODO: there's a better way to do this: poll the queue repeatedly with a timeout
      // TODO: see https://github.com/zio/zio/blob/31d9eacbb400c668460735a8a44fb68af9e5c311/core-tests/shared/src/test/scala/zio/ZQueueSpec.scala#L862 fo
      _             <- sleepDuration.map(Clock.Service.live.sleep(_)).getOrElse(ZIO.unit)
      testResult    <- requestsAssertion(requestQueue)
    } yield testResult

  def createWebhooks(n: Int)(status: WebhookStatus, deliveryMode: WebhookDeliveryMode): Iterable[Webhook] =
    (0 until n).map { i =>
      Webhook(
        WebhookId(i.toLong),
        "http://example.org/" + i,
        "testWebhook" + i,
        status,
        deliveryMode
      )
    }

  def createWebhookEvents(n: Int)(webhookId: WebhookId): Iterable[WebhookEvent] =
    (0 until n).map { i =>
      WebhookEvent(
        WebhookEventKey(WebhookEventId(i.toLong), webhookId),
        WebhookEventStatus.New,
        "lorem ipsum " + i,
        Chunk(("Accept", "*/*"))
      )
    }

  type TestEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[TestWebhookHttpClient]
    with Has[WebhookHttpClient]
    with Has[WebhookServer]

  val testEnv: ULayer[TestEnv] = {
    val repos =
      (TestWebhookRepo.test >+> TestWebhookEventRepo.test) ++ TestWebhookStateRepo.test ++ TestWebhookHttpClient.test
    repos ++ (repos >>> WebhookServer.live)
  }.orDie
}
