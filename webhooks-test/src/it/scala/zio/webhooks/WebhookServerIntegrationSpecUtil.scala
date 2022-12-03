package zio.webhooks

import zhttp.http._
import zhttp.service.Server
import zio.stream.{ SubscriptionRef, ZStream }
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.backends.{ InMemoryWebhookStateRepo, JsonPayloadSerialization }
import zio.webhooks.testkit.{ TestWebhookEventRepo, TestWebhookRepo }
import zio._
import zio.json._

object WebhookServerIntegrationSpecUtil {

  lazy val port = 8081

  // limit max backoff to 1 second so tests don't take too long
  def customConfig =
    WebhookServerConfig.default.update { config =>
      config.copy(
        retry = config.retry.copy(
          maxBackoff = 1.second
        )
      )
    }

  def events(webhookIdRange: (Int, Int)): ZStream[Any, Nothing, WebhookEvent] =
    ZStream
      .iterate(0L)(_ + 1)
      .zip(ZStream.repeatZIO(Random.nextIntBetween(webhookIdRange._1, webhookIdRange._2)))
      .map {
        case (i, webhookId) =>
          WebhookEvent(
            WebhookEventKey(WebhookEventId(i), WebhookId(webhookId.toLong)),
            WebhookEventStatus.New,
            i.toString, // a single number string is valid JSON
            Chunk(("Accept", "*/*"), ("Content-Type", "application/json")),
            None
          )
      }

  def singleAtMostOnceEvents(n: Long) =
    events(webhookIdRange = (0, 250)).take(n / 4)

  def batchedAtMostOnceEvents(n: Long) =
    events(webhookIdRange = (250, 500)).drop(n.toInt / 4).take(n / 4)

  def singleAtLeastOnceEvents(n: Long) =
    events(webhookIdRange = (500, 750)).drop(n.toInt / 2).take(n / 4)

  def batchedAtLeastOnceEvents(n: Long) =
    events(webhookIdRange = (750, 1000)).drop(3 * n.toInt / 4).take(n / 4)

  type IntegrationEnv = WebhookEventRepo
    with TestWebhookEventRepo
    with WebhookRepo
    with TestWebhookRepo
    with WebhookStateRepo
    with WebhookHttpClient
    with WebhooksProxy
    with WebhookServerConfig
    with SerializePayload

  // alias for zio-http endpoint server
  lazy val httpEndpointServer = Server

  lazy val integrationEnv: ULayer[IntegrationEnv] =
    ZLayer
      .make[IntegrationEnv](
        InMemoryWebhookStateRepo.live,
        JsonPayloadSerialization.live,
        TestWebhookEventRepo.test,
        TestWebhookRepo.subscriptionUpdateMode,
        TestWebhookRepo.test,
        WebhookServerConfig.default,
        WebhookSttpClient.live,
        WebhooksProxy.live
      )
      .orDie

  def reliableEndpoint(delivered: SubscriptionRef[Set[Int]]) =
    Http.collectZIO[Request] {
      case request @ Method.POST -> !! / "endpoint" / (id @ _) =>
        for {
          randomDelay <- Random.nextIntBounded(200).map(_.millis)
          response    <- request.body.asString.flatMap { body =>
                           val singlePayload = body.fromJson[Int].map(Left(_))
                           val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                           val payload       = singlePayload.orElseThat(batchPayload).toOption
                           ZIO.foreachDiscard(payload) {
                             case Left(i)   =>
                               delivered.updateZIO(set => ZIO.succeed(set + i))
                             case Right(is) =>
                               delivered.updateZIO(set => ZIO.succeed(set ++ is))
                           }
                         }
                           .as(Response.status(Status.Ok))
                           .delay(randomDelay) // simulate network/server latency
        } yield response
    }

  def slowEndpointsExceptFirst(delivered: SubscriptionRef[Set[Int]]) =
    Http.collectZIO[Request] {
      case request @ Method.POST -> !! / "endpoint" / id if id == "0" =>
        for {
          _        <- request.body.asString.flatMap { body =>
                        val singlePayload = body.fromJson[Int].map(Left(_))
                        val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                        val payload       = singlePayload.orElseThat(batchPayload).toOption
                        ZIO
                          .foreachDiscard(payload) {
                            case Left(i)   =>
                              delivered.updateZIO(set => ZIO.succeed(set + i))
                            case Right(is) =>
                              delivered.updateZIO(set => ZIO.succeed(set ++ is))
                          }
                      }
          response <- ZIO.succeed(Response.status(Status.Ok))
        } yield response
      case _                                                          =>
        ZIO.succeed(Response.status(Status.Ok)).delay(1.minute)
    }

  lazy val testWebhooks: IndexedSeq[Webhook] = (0 until 250).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtMostOnce,
      None
    )
  } ++ (250 until 500).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtMostOnce,
      None
    )
  } ++ (500 until 750).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.SingleAtLeastOnce,
      None
    )
  } ++ (750 until 1000).map { i =>
    Webhook(
      id = WebhookId(i.toLong),
      url = s"http://0.0.0.0:$port/endpoint/$i",
      label = s"test webhook $i",
      WebhookStatus.Enabled,
      WebhookDeliveryMode.BatchedAtLeastOnce,
      None
    )
  }

  lazy val webhookCount = 1000
}
