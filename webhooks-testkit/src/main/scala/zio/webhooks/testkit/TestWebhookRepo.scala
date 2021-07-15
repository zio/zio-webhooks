package zio.webhooks.testkit

import zio._
import zio.webhooks.WebhookError._
import zio.webhooks._

trait TestWebhookRepo {
  def createWebhook(webhook: Webhook): UIO[Unit]

  def getWebhooks: UManaged[Dequeue[Webhook]]
}

object TestWebhookRepo {
  // Layer Definitions

  val test: ULayer[Has[TestWebhookRepo] with Has[WebhookRepo]] = {
    for {
      ref <- Ref.make(Map.empty[WebhookId, Webhook])
      hub <- Hub.unbounded[Webhook]
      impl = TestWebhookRepoImpl(ref, hub)
    } yield Has.allOf[TestWebhookRepo, WebhookRepo](impl, impl)
  }.toLayerMany

  // Accessor Methods

  def createWebhook(webhook: Webhook): URIO[Has[TestWebhookRepo], Unit] =
    ZIO.serviceWith(_.createWebhook(webhook))

  def getWebhooks: URManaged[Has[TestWebhookRepo], Dequeue[Webhook]] =
    ZManaged.service[TestWebhookRepo].flatMap(_.getWebhooks)
}

final private case class TestWebhookRepoImpl(ref: Ref[Map[WebhookId, Webhook]], hub: Hub[Webhook])
    extends WebhookRepo
    with TestWebhookRepo {

  def createWebhook(webhook: Webhook): UIO[Unit] =
    ref.update(_ + ((webhook.id, webhook))) <* hub.publish(webhook)

  def getWebhooks: UManaged[Dequeue[Webhook]] =
    hub.subscribe

  def getWebhookById(webhookId: WebhookId): IO[MissingWebhookError, Webhook] =
    ref.get.map(_.get(webhookId)).flatMap(ZIO.fromOption(_).orElseFail(MissingWebhookError(webhookId)))

  def setWebhookStatus(id: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit] =
    for {
      webhookExists <- ref.modify { map =>
                         map.get(id) match {
                           case None          =>
                             (None, map)
                           case Some(webhook) =>
                             val updatedWebhook = webhook.copy(status = status)
                             (Some(updatedWebhook), map.updated(id, updatedWebhook))
                         }
                       }
      _             <- webhookExists match {
                         case Some(updatedWebhook) => hub.publish(updatedWebhook)
                         case None                 => ZIO.fail(MissingWebhookError(id))
                       }
    } yield ()
}
