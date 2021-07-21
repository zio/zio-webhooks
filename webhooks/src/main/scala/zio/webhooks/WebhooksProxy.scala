package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhookError.MissingWebhookError
import zio.webhooks.WebhooksProxy.UpdateMode
import zio.webhooks.WebhooksProxy.UpdateMode.PollingFunction

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

  private def pollForUpdates(pollingFunction: PollingFunction): UIO[Unit] =
    for {
      keys <- cache.get.map(map => NonEmptySet.fromIterableOption(map.keys))
      _    <- ZIO.foreach_(keys)(pollingFunction(_).flatMap(cache.set))
    } yield ()

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

  private[webhooks] def start: URIO[Clock, Unit] =
    for {
      _ <- updateMode match {
             case UpdateMode.Polling(pollingInterval, pollingFunction) =>
               pollForUpdates(pollingFunction).repeat(Schedule.fixed(pollingInterval))
             case UpdateMode.Subscription(subscription)                =>
               subscription.foreach {
                 case WebhookUpdate.WebhookRemoved(webhookId) =>
                   cache.update(_ - webhookId)
                 case WebhookUpdate.WebhookChanged(webhook)   =>
                   // we only update if the webhook is relevant i.e., getWebhook was called to get it before
                   cache.update(_.updateWith(webhook.id)(_.map(_ => webhook)))
               }
           }
    } yield ()
}

object WebhooksProxy {
  type Env = Has[WebhookRepo] with Has[UpdateMode] with Clock

  val live: URLayer[Env, Has[WebhooksProxy]] =
    ZIO.services[WebhookRepo, UpdateMode].flatMap((start _).tupled).toLayer

  private def start(webhookRepo: WebhookRepo, updateMode: UpdateMode): URIO[Clock, WebhooksProxy] =
    for {
      cache <- Ref.make(Map.empty[WebhookId, Webhook])
      proxy  = new WebhooksProxy(cache, webhookRepo, updateMode)
      _     <- proxy.start.fork
    } yield proxy

  sealed trait UpdateMode extends Product with Serializable
  object UpdateMode {
    final case class Polling(interval: Duration, f: PollingFunction) extends UpdateMode

    type PollingFunction = NonEmptySet[WebhookId] => UIO[Map[WebhookId, Webhook]]

    final case class Subscription(value: UStream[WebhookUpdate]) extends UpdateMode
  }
}
