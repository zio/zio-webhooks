package zio.webhooks

import WebhookError._
import zio.{ Has, IO, Ref, Task, ULayer, ZIO }

final case class TestWebhookRepo(
  ref: Ref[Map[WebhookId, Webhook]]
) extends WebhookRepo {

  def getWebhookById(webhookId: WebhookId): Task[Option[Webhook]] =
    ref.get.map(_.get(webhookId))

  def setWebhookStatus(id: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit] =
    for {
      webhookExists <- ref.modify { map =>
                         map.get(id) match {
                           case None          =>
                             (false, map)
                           case Some(webhook) =>
                             (true, map.updated(id, webhook.copy(status = status)))
                         }
                       }
      _             <- ZIO.unless(webhookExists)(ZIO.fail(MissingWebhookError(id)))
    } yield ()
}

object TestWebhookRepo {
  val testLayer: ULayer[Has[WebhookRepo]] = Ref.make(Map.empty[WebhookId, Webhook]).map(TestWebhookRepo(_)).toLayer
}
