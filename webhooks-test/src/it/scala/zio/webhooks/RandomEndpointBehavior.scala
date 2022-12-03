package zio.webhooks

import zhttp.http._
import zhttp.service.Server
import zio.Console.printLine
import zio.stream.{ SubscriptionRef, ZStream }
import zio._
import zio.json._
import WebhookServerIntegrationSpecUtil.port

object RandomEndpointBehavior {
  case object Down  extends RandomEndpointBehavior
  case object Flaky extends RandomEndpointBehavior

  def flakyBehavior(delivered: SubscriptionRef[Set[Int]]) =
    Http.collectZIO[Request] {
      case request @ Method.POST -> !! / "endpoint" / (id @ _) =>
        for {
          n           <- Random.nextIntBounded(100)
          randomDelay <- Random.nextIntBounded(200).map(_.millis)
          response    <- request.body.asString.flatMap { body =>
                           val singlePayload = body.fromJson[Int].map(Left(_))
                           val batchPayload  = body.fromJson[List[Int]].map(Right(_))
                           val payload       = singlePayload.orElseThat(batchPayload).toOption
                           if (n < 60)
                             ZIO
                               .foreachDiscard(payload) {
                                 case Left(i)   =>
                                   delivered.updateZIO(set => ZIO.succeed(set + i))
                                 case Right(is) =>
                                   delivered.updateZIO(set => ZIO.succeed(set ++ is))
                               }
                               .as(Response.status(Status.Ok))
                           else
                             ZIO.succeed(Response.status(Status.NotFound))
                         }
                           .delay(randomDelay)
        } yield response
    }

  // just an alias for a zio-http server to tell it apart from the webhook server
  lazy val httpEndpointServer: Server.type = Server

  val randomBehavior: UIO[RandomEndpointBehavior] =
    Random.nextIntBounded(100).map(n => if (n < 80) Flaky else Down)

  def run(delivered: SubscriptionRef[Set[Int]]) =
    ZStream.repeatZIO(randomBehavior).foreach { behavior =>
      for {
        _ <- printLine(s"Endpoint server behavior: $behavior")
        f <- behavior.start(delivered).fork
        _ <- behavior match {

               case Down  =>
                 f.interrupt.delay(2.seconds)
               case Flaky =>
                 f.interrupt.delay(10.seconds)
             }
      } yield ()
    }
}

sealed trait RandomEndpointBehavior extends Product with Serializable { self =>
  import RandomEndpointBehavior._

  def start(delivered: SubscriptionRef[Set[Int]]) =
    self match {
      case RandomEndpointBehavior.Down  =>
        ZIO.unit
      case RandomEndpointBehavior.Flaky =>
        httpEndpointServer.start(port, flakyBehavior(delivered))
    }
}
