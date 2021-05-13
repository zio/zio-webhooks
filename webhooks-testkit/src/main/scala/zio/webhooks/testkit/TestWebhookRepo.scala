package zio.webhooks

import zio.{ Ref, Task }

final case class TestWebhookRepo(
  ref: Ref[Map[WebhookId, Webhook]]
) extends WebhookRepo {
  def getWebhookById(webhookId: WebhookId): Task[Option[Webhook]] = ???

  def setWebhookStatus(id: WebhookId, status: WebhookStatus): Task[Unit] = ???
}

object TestWebhookRepo {
  // make a layer that produces a WebhookRepo
}
