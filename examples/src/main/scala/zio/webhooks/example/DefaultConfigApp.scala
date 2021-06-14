package zio.webhooks.example

import zio._
import zio.console._
import zio.magic._
import zio.stream.UStream
import zio.webhooks._
import zio.webhooks.backends.sttp.WebhookSttpClient
import zio.webhooks.testkit._

import java.io.IOException

object DefaultConfigApp extends App {

  private def program: ZIO[Console with Has[WebhookServer], IOException, Unit] =
    for {
      _ <- WebhookServer.getErrors.use(UStream.fromQueue(_).map(_.toString).foreach(putStrLnErr(_)))
      // TODO: add webhooks, events, zio-http server with basic endpoint
    } yield ()

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
      .injectSome[ZEnv](
        WebhookServer.live,
        TestWebhookRepo.test,
        TestWebhookStateRepo.test,
        TestWebhookEventRepo.test,
        WebhookSttpClient.live,
        WebhookServerConfig.default
      )
      .exitCode
}
