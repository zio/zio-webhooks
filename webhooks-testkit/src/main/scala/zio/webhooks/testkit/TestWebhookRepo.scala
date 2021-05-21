package zio.webhooks.testkit

import zio._
import zio.webhooks.WebhookError._
import zio.webhooks._

trait TestWebhookRepo {
  def createWebhook(webhook: Webhook): UIO[Unit]
}

object TestWebhookRepo {
  // Layer Definitions

  val test: ULayer[Has[TestWebhookRepo] with Has[WebhookRepo]] = {
    for {
      ref <- Ref.make(Map.empty[WebhookId, Webhook])
      impl = TestWebhookRepoImpl(ref)
    } yield Has.allOf[TestWebhookRepo, WebhookRepo](impl, impl)
  }.toLayerMany

  // Accessor Methods

  def createWebhook(webhook: Webhook): URIO[Has[TestWebhookRepo], Unit] =
    ZIO.serviceWith(_.createWebhook(webhook))
}

final private case class TestWebhookRepoImpl(
  ref: Ref[Map[WebhookId, Webhook]]
) extends WebhookRepo
    with TestWebhookRepo {

  def createWebhook(webhook: Webhook): UIO[Unit] =
    ref.update(_ + ((webhook.id, webhook)))

  def getWebhookById(webhookId: WebhookId): UIO[Option[Webhook]] =
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
