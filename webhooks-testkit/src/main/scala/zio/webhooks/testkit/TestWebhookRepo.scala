package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookUpdate.WebhookChanged
import zio.webhooks._

trait TestWebhookRepo {
  def removeWebhook(webhookId: WebhookId): UIO[Unit]

  def setWebhook(webhook: Webhook): UIO[Unit]
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

  def setWebhook(webhook: Webhook): URIO[Has[TestWebhookRepo], Unit] =
    ZIO.serviceWith(_.setWebhook(webhook))
}

final private case class TestWebhookRepoImpl(ref: Ref[Map[WebhookId, Webhook]], hub: Hub[Webhook])
    extends WebhookRepo
    with TestWebhookRepo {

  def getWebhookById(webhookId: WebhookId): UIO[Option[Webhook]] =
    ref.get.map(_.get(webhookId))

  def pollWebhooksById(webhookIds: NonEmptySet[WebhookId]): UIO[Map[WebhookId, Webhook]] =
    ref.get.map(_.filter { case (id, _) => webhookIds.contains(id) })

  def removeWebhook(webhookId: WebhookId): UIO[Unit]                                     =
    ref.update(_ - webhookId)

  def setWebhook(webhook: Webhook): UIO[Unit]                                                      =
    ref.update(_ + (webhook.id -> webhook)) <* hub.publish(webhook)

  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit] =
    for {
      webhookExists <- ref.modify { map =>
                         map.get(webhookId) match {
                           case None          =>
                             (None, map)
                           case Some(webhook) =>
                             val updatedWebhook = webhook.copy(status = status)
                             (Some(updatedWebhook), map.updated(webhookId, updatedWebhook))
                         }
                       }
      _             <- webhookExists match {
                         case Some(updatedWebhook) => hub.publish(updatedWebhook)
                         case None                 => ZIO.fail(MissingWebhookError(webhookId))
                       }
    } yield ()

  def subscribeToWebhookUpdates: UManaged[Dequeue[WebhookUpdate]] =
    hub.subscribe.map(_.map(WebhookChanged))
}
