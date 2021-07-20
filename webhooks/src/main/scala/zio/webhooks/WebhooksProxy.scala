package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.prelude.NonEmptySet
import zio.webhooks.WebhookError.MissingWebhookError
import zio.webhooks.WebhooksProxy.UpdateMode

/**
 * Mediates access to [[Webhook]]s, caching webhooks in memory while keeping them updated by polling
 * for some interval or with a subscription.
 */
final class WebhooksProxy private (
  private val cache: Ref[Map[WebhookId, Webhook]],
  private val webhookRepo: WebhookRepo,
  private val updateMode: UpdateMode
) {

  /**
   * Looks up a webhook from the server's internal webhook map by [[WebhookId]]. If missing, we look
   * for it in the [[WebhookRepo]], raising a [[MissingWebhookError]] if we don't find one there.
   * Adds webhooks looked up from a repo to the server's internal webhook map.
   */
  def getWebhookById(webhookId: WebhookId): IO[MissingWebhookError, Webhook] =
    for {
      option  <- cache.get.map(_.get(webhookId))
      webhook <- option match {
                   case Some(webhook) =>
                     UIO(webhook)
                   case None          =>
                     for {
                       webhook <- webhookRepo
                                    .getWebhookById(webhookId)
                                    .flatMap(ZIO.fromOption(_).orElseFail(MissingWebhookError(webhookId)))
                       _       <- cache.update(_ + (webhookId -> webhook))
                     } yield webhook
                 }
    } yield webhook

  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): IO[MissingWebhookError, Unit] =
    for {
      webhookExists <- cache.modify(map =>
                         map.get(webhookId) match {
                           case Some(webhook) =>
                             val updatedWebhook = webhook.copy(status = status)
                             (true, map.updated(webhookId, updatedWebhook))
                           case None          =>
                             (false, map)
                         }
                       )
      _             <- if (webhookExists)
                         webhookRepo.setWebhookStatus(webhookId, status)
                       else
                         ZIO.fail(MissingWebhookError(webhookId))

    } yield ()

  private def update: URIO[Clock, WebhooksProxy] =
    for {
      _ <- updateMode match {
             case UpdateMode.Polling(pollingInterval, pollingFunction) =>
               val loop = for {
                 keys <- cache.get.map(map => NonEmptySet.fromIterableOption(map.keys))
                 _    <- ZIO.foreach_(keys) { keys =>
                           for {
                             updates <- pollingFunction(keys)
                             _       <- cache.set(updates)
                           } yield ()
                         }
               } yield ()
               loop.repeat(Schedule.fixed(pollingInterval))
             case UpdateMode.Subscription(subscription)                =>
               subscription.use { queue =>
                 val loop =
                   for {
                     update <- queue.take
                     _      <- update match {
                                 case WebhookUpdate.WebhookRemoved(webhookId) =>
                                   cache.update(_ - webhookId)
                                 case WebhookUpdate.WebhookChanged(webhook)   =>
                                   cache.update(_ + (webhook.id -> webhook))
                               }
                   } yield ()
                 loop.forever.fork
               }
           }
    } yield this
}

object WebhooksProxy {
  type Env = Has[WebhookRepo] with Has[UpdateMode] with Clock

  val live: URLayer[Env, Has[WebhooksProxy]] =
    ZIO.services[WebhookRepo, UpdateMode].flatMap((start _).tupled).toLayer

  private def start(webhookRepo: WebhookRepo, updateMode: UpdateMode): URIO[Clock, WebhooksProxy] =
    Ref.make(Map.empty[WebhookId, Webhook]).flatMap(new WebhooksProxy(_, webhookRepo, updateMode).update)

  sealed trait UpdateMode extends Product with Serializable
  object UpdateMode {
    final case class Polling(interval: Duration, f: PollingFunction) extends UpdateMode

    type PollingFunction = (NonEmptySet[WebhookId]) => UIO[Map[WebhookId, Webhook]]

    final case class Subscription(value: UManaged[Dequeue[WebhookUpdate]]) extends UpdateMode
  }
}
