package zio.webhooks

import zio.{ Has, Ref, Task, ULayer }

final case class TestWebhookRepo(
  ref: Ref[Map[WebhookId, Webhook]]
) extends WebhookRepo {
  def getWebhookById(webhookId: WebhookId): Task[Option[Webhook]] = ???

  def setWebhookStatus(id: WebhookId, status: WebhookStatus): Task[Unit] = ???
}

object TestWebhookRepo {
  val test: ULayer[Has[WebhookRepo]] = Ref.make(Map.empty[WebhookId, Webhook]).map(TestWebhookRepo(_)).toLayer
}
