package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhookError._
import zio.webhooks.WebhookUpdate._
import zio.webhooks.WebhooksProxy.UpdateMode
import zio.webhooks._

trait TestWebhookRepo {
  def getWebhookUpdates: UStream[WebhookUpdate]

  def removeWebhook(webhookId: WebhookId): UIO[Unit]

  /**
   * Polls webhooks by id for updates. Implementations must return a map of webhookIds to webhooks.
   */
  def pollWebhooksById(webhookIds: NonEmptySet[WebhookId]): UIO[Map[WebhookId, Webhook]]

  def setWebhook(webhook: Webhook): UIO[Unit]

  /**
   * Subscribes to webhook updates.
   */
  def subscribeToWebhookUpdates: UManaged[Dequeue[WebhookUpdate]]
}

object TestWebhookRepo {

  def removeWebhook(webhookId: WebhookId): URIO[Has[TestWebhookRepo], Unit] =
    ZIO.serviceWith(_.removeWebhook(webhookId))

  val test: ULayer[Has[TestWebhookRepo] with Has[WebhookRepo]] = {
    for {
      ref <- Ref.make(Map.empty[WebhookId, Webhook])
      hub <- Hub.bounded[WebhookUpdate](256)
      impl = TestWebhookRepoImpl(ref, hub)
    } yield Has.allOf[TestWebhookRepo, WebhookRepo](impl, impl)
  }.toLayerMany

  val subscriptionUpdateMode: URLayer[Has[TestWebhookRepo], Has[UpdateMode]] =
    ZIO.service[TestWebhookRepo].map(repo => UpdateMode.Subscription(repo.getWebhookUpdates)).toLayer

  def setWebhook(webhook: Webhook): URIO[Has[TestWebhookRepo], Unit] =
    ZIO.serviceWith(_.setWebhook(webhook))

  def subscribeToWebhooks: URManaged[Has[TestWebhookRepo], Dequeue[WebhookUpdate]] =
    ZManaged.service[TestWebhookRepo].flatMap(_.subscribeToWebhookUpdates)
}

final private case class TestWebhookRepoImpl(ref: Ref[Map[WebhookId, Webhook]], hub: Hub[WebhookUpdate])
    extends WebhookRepo
    with TestWebhookRepo {

  def getWebhookById(webhookId: WebhookId): UIO[Option[Webhook]] =
    ref.get.map(_.get(webhookId))

  def getWebhookUpdates: UStream[WebhookUpdate] =
    UStream.fromHub(hub)

  def pollWebhooksById(webhookIds: NonEmptySet[WebhookId]): UIO[Map[WebhookId, Webhook]] =
    ref.get.map(_.filter { case (id, _) => webhookIds.contains(id) })

  def removeWebhook(webhookId: WebhookId): UIO[Unit]                                     =
    ref.update(_ - webhookId) <* hub.publish(WebhookRemoved(webhookId))

  def setWebhook(webhook: Webhook): UIO[Unit]                                                      =
    ref.update(_ + (webhook.id -> webhook)) <* hub.publish(WebhookChanged(webhook))

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
                         case Some(updatedWebhook) => hub.publish(WebhookChanged(updatedWebhook))
                         case None                 => ZIO.fail(MissingWebhookError(webhookId))
                       }
    } yield ()

  def subscribeToWebhookUpdates: UManaged[Dequeue[WebhookUpdate]] =
    hub.subscribe
}
