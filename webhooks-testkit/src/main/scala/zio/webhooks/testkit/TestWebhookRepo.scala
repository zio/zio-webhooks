package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream.{ UStream, ZStream }
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
  def subscribeToWebhookUpdates: URIO[Scope, Dequeue[WebhookUpdate]]
}

object TestWebhookRepo {

  def removeWebhook(webhookId: WebhookId): URIO[TestWebhookRepo, Unit] =
    ZIO.serviceWithZIO(_.removeWebhook(webhookId))

  val test: ULayer[TestWebhookRepo with WebhookRepo] =
    ZLayer.fromZIO {
      for {
        ref <- Ref.make(Map.empty[WebhookId, Webhook])
        hub <- Hub.bounded[WebhookUpdate](256)
      } yield TestWebhookRepoImpl(ref, hub)
    }

  val subscriptionUpdateMode: URLayer[TestWebhookRepo, UpdateMode] =
    ZLayer.fromZIO(ZIO.service[TestWebhookRepo].map(repo => UpdateMode.Subscription(repo.getWebhookUpdates)))

  def setWebhook(webhook: Webhook): URIO[TestWebhookRepo, Unit] =
    ZIO.serviceWithZIO(_.setWebhook(webhook))

  def subscribeToWebhooks: URIO[Scope with TestWebhookRepo, Dequeue[WebhookUpdate]] =
    ZIO.serviceWithZIO[TestWebhookRepo](_.subscribeToWebhookUpdates)
}

final private case class TestWebhookRepoImpl(ref: Ref[Map[WebhookId, Webhook]], hub: Hub[WebhookUpdate])
    extends WebhookRepo
    with TestWebhookRepo {

  def getWebhookById(webhookId: WebhookId): UIO[Webhook] =
    ref.get.map(_(webhookId))

  def getWebhookUpdates: UStream[WebhookUpdate] =
    ZStream.fromHub(hub)

  def pollWebhooksById(webhookIds: NonEmptySet[WebhookId]): UIO[Map[WebhookId, Webhook]] =
    ref.get.map(_.filter { case (id, _) => webhookIds.contains(id) })

  def removeWebhook(webhookId: WebhookId): UIO[Unit]                                     =
    ref.update(_ - webhookId) <* hub.publish(WebhookRemoved(webhookId))

  def setWebhook(webhook: Webhook): UIO[Unit]                                  =
    ref.update(_ + (webhook.id -> webhook)) <* hub.publish(WebhookChanged(webhook))

  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): UIO[Unit] =
    for {
      updatedWebhook <- ref.modify { map =>
                          val webhook        = map(webhookId)
                          val updatedWebhook = webhook.copy(status = status)
                          (updatedWebhook, map.updated(webhookId, updatedWebhook))
                        }
      _              <- hub.publish(WebhookChanged(updatedWebhook))
    } yield ()

  def subscribeToWebhookUpdates: URIO[Scope, Dequeue[WebhookUpdate]] =
    hub.subscribe
}
