package zio.webhooks.testkit

import zio.webhooks.WebhookStateRepo
import zio.Ref
import zio.Task
import zio.{ Has, ULayer }

final case class TestWebhookStateRepo(
  ref: Ref[String]
) extends WebhookStateRepo {

  def getState: Task[String] = ref.get

  def setState(state: String): Task[Unit] = ref.set(state)
}

object TestWebhookStateRepo {
  val testLayer: ULayer[Has[WebhookStateRepo]] = Ref.make("").map(TestWebhookStateRepo(_)).toLayer
}
