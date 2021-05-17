package zio.webhooks.testkit

import zio._
import zio.webhooks.WebhookStateRepo

final case class TestWebhookStateRepo(
  ref: Ref[Option[String]]
) extends WebhookStateRepo {

  def getState: UIO[Option[String]] = ref.get

  def setState(state: String): Task[Unit] = ref.set(Some(state))
}

object TestWebhookStateRepo {
  val testLayer: ULayer[Has[WebhookStateRepo]] = Ref.make("").map(TestWebhookStateRepo(_)).toLayer
}
