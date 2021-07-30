package zio.webhooks

import zhttp.http._
import zhttp.service.Server
import zio._
import zio.clock.Clock
import zio.duration._
import zio.json._
import zio.magic._
import zio.random.Random
import zio.stream._
import zio.test.Assertion._
import zio.test._
import zio.webhooks.WebhookServerIntegrationSpecUtil._
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._

object WebhookServerIntegrationSpec extends DefaultRunnableSpec {
  val spec =
    suite("WebhookServerIntegrationSpec") {
      testM("all events are delivered eventually") {
        val n = 10000L

        {
          for {
            _              <- ZIO.foreach_(testWebhooks)(TestWebhookRepo.setWebhook)
            delivered      <- SubscriptionRef.make(Set.empty[Int])
            _              <- WebhookServer.start
            reliableServer <- httpEndpointServer.start(port, reliableEndpoint(delivered)).fork
            // send events for webhooks with single delivery, at-most-once semantics
            _              <- events(webhookIdRange = (0, 250))
                                .take(n / 4)
                                // pace events so we don't overwhelm the endpoint
                                .schedule(Schedule.spaced(20.micros))
                                .foreach(TestWebhookEventRepo.createEvent)
            // send events for webhooks with batched delivery, at-most-once semantics
            // no need to pace events as batching minimizes requests sent (we can go as fast as we want)
            _              <- events(webhookIdRange = (250, 512))
                                .drop(n / 4)
                                .take(n / 4)
                                .foreach(TestWebhookEventRepo.createEvent)
            result         <- delivered.changes.filter(_.size == n / 2).runHead
            _              <- reliableServer.interrupt
          } yield assert(result)(isSome(anything))
        }.provideSomeLayer[IntegrationEnv with Random](Clock.live)
      }
    }.injectCustom(integrationEnv)
}

object WebhookServerIntegrationSpecUtil {

  // backport for 2.12
  implicit class EitherOps[A, B](either: Either[A, B]) {

    def orElseThat[A1 >: A, B1 >: B](or: => Either[A1, B1]): Either[A1, B1] = either match {
      case Right(_) => either
      case _        => or
    }
  }

  def events(webhookIdRange: (Int, Int)): ZStream[Random, Nothing, WebhookEvent] =
    UStream
      .iterate(0L)(_ + 1)
      .zip(UStream.repeatEffect(random.nextIntBetween(webhookIdRange._1, webhookIdRange._2)))
      .map {
        case (i, webhookId) =>
          WebhookEvent(
            WebhookEventKey(WebhookEventId(i), WebhookId(webhookId.toLong)),
            WebhookEventStatus.New,
            i.toString, // a single number string is valid JSON
            Chunk(("Accept", "*/*"), ("Content-Type", "application/json"))
          )
      }

  type IntegrationEnv = Has[WebhookEventRepo]
    with Has[TestWebhookEventRepo]
    with Has[WebhookRepo]
    with Has[TestWebhookRepo]
    with Has[WebhookStateRepo]
    with Has[WebhookHttpClient]
    with Has[WebhooksProxy]
    with Has[WebhookServerConfig]

  // alias for zio-http endpoint server
  lazy val httpEndpointServer = Server

  lazy val integrationEnv: URLayer[Clock, IntegrationEnv] =
    ZLayer
      .wireSome[Clock, IntegrationEnv](
        TestWebhookEventRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        WebhookServerConfig.default,
        WebhookSttpClient.live,
        WebhooksProxy.live
      )
      .orDie

  lazy val port = 8081

  def reliableEndpoint(delivered: SubscriptionRef[Set[Int]]) =
    HttpApp.collectM {
      case request @ Method.POST -> Root / "endpoint" / (id @ _) =>
        val response =
          for {
            randomDelay <- random.nextIntBounded(200).map(_.millis)
            response    <- ZIO
                             .foreach_(request.getBodyAsString) { body =>
                               val singlePayload = body.fromJson[Int].map(Left(_))
                               val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                               val payload       = singlePayload.orElseThat(batchPayload).toOption
                               ZIO.foreach_(payload) {
                                 case Left(i)   =>
                                   delivered.ref.update(set => UIO(set + i))
                                 case Right(is) =>
                                   delivered.ref.update(set => UIO(set ++ is))
                               }
                             }
                             .as(Response.status(Status.OK))
                             .delay(randomDelay) // simulate network/server latency
          } yield response
        response.uninterruptible
    }

  lazy val testWebhooks: IndexedSeq[Webhook] = (0 until 250).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce
    )
  } ++ (250 until 500).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtMostOnce
    )
  } ++ (500 until 750).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtLeastOnce
    )
  } ++ (750 until 1000).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtLeastOnce
    )
  }

  lazy val webhookCount = 1000
}
