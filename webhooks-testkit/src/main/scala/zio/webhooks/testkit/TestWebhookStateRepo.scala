package zio.webhooks.testkit

import zio._
import zio.webhooks.WebhookStateRepo

final case class TestWebhookStateRepo(ref: Ref[Option[String]]) extends WebhookStateRepo {

  def loadState: UIO[Option[String]] = ref.modify((_, None))

  def setState(state: String): UIO[Unit] = ref.set(Some(state))
}

object TestWebhookStateRepo {
  val test: ULayer[WebhookStateRepo] =
    Ref.make(Option.empty[String]).map(TestWebhookStateRepo(_)).toLayer
}
