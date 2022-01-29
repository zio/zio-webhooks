package zio.webhooks.backends

import zio._
import zio.webhooks.WebhookStateRepo

final case class InMemoryWebhookStateRepo private (ref: Ref[Option[String]]) extends WebhookStateRepo {

  def loadState: UIO[Option[String]] = ref.modify((_, None))

  def setState(state: String): UIO[Unit] = ref.set(Some(state))
}

object InMemoryWebhookStateRepo {
  val live: ULayer[WebhookStateRepo] =
    Ref.make[Option[String]](Option.empty).map(InMemoryWebhookStateRepo(_)).toLayer
}
