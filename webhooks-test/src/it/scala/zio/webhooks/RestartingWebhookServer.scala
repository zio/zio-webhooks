package zio.webhooks

import zio.Console.{ printError, printLine }
import zio.stream.ZStream
import zio.webhooks.testkit.TestWebhookEventRepo
import zio._

object RestartingWebhookServer {

  def start =
    runServerThenShutdown.forever

  private def runServerThenShutdown =
    for {
      _ <- printLine("Server starting")
      _ <- ZIO.scoped {
             WebhookServer.start.flatMap { server =>
               for {
                 _        <- printLine("Server started")
                 f        <- server.subscribeToErrors
                               .flatMap(ZStream.fromQueue(_).map(_.toString).foreach(printError(_)))
                               .forkScoped
                 _        <- TestWebhookEventRepo.enqueueNew
                 duration <- Random.nextIntBetween(3000, 5000).map(_.millis)
                 _        <- f.interrupt.delay(duration)
               } yield ()
             }
           }
      _ <- printLine("Server shut down")
    } yield ()
}
