package zio.webhooks

import zio._
import zio.clock.Clock
import zio.duration.Duration
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhooksProxy.UpdateMode
import zio.webhooks.WebhooksProxy.UpdateMode.PollingFunction

/**
 * Mediates access to [[Webhook]]s, caching webhooks in memory while keeping them updated by polling
 * for some interval or with a subscription.
 */
final case class WebhooksProxy private (
  private val cache: Ref[Map[WebhookId, Webhook]],
  private val webhookRepo: WebhookRepo,
  private val updateMode: UpdateMode
) {

  /**
   * Looks up a webhook from the server's internal webhook map by [[WebhookId]]. If missing, we look
   * it up in the [[WebhookRepo]]. Adds webhooks looked up from a repo to the server's internal
   * webhook map.
   */
  def getWebhookById(webhookId: WebhookId): UIO[Webhook] =
    for {
      option  <- cache.get.map(_.get(webhookId))
      webhook <- option.map(UIO(_)).getOrElse(webhookRepo.getWebhookById(webhookId))
    } yield webhook

  private def pollForUpdates(pollingFunction: PollingFunction): UIO[Unit] =
    for {
      keys <- cache.get.map(map => NonEmptySet.fromIterableOption(map.keys))
      _    <- ZIO.foreach_(keys)(pollingFunction(_).flatMap(cache.set))
    } yield ()

  def setWebhookStatus(webhookId: WebhookId, status: WebhookStatus): UIO[Unit] =
    for {
      _ <- cache.update(map =>
             map.get(webhookId).fold(map)(webhook => map.updated(webhookId, webhook.copy(status = status)))
           )
      _ <- webhookRepo.setWebhookStatus(webhookId, status)
    } yield ()

  private[webhooks] def start: URIO[Clock, Any] =
    updateMode match {
      case UpdateMode.Polling(pollingInterval, pollingFunction) =>
        pollForUpdates(pollingFunction).repeat(Schedule.fixed(pollingInterval))
      case UpdateMode.Subscription(subscription)                =>
        subscription.foreach {
          case WebhookUpdate.WebhookRemoved(webhookId) =>
            cache.update(_ - webhookId)
          case WebhookUpdate.WebhookChanged(webhook)   =>
            // we only update relevant webhooks, i.e. getWebhook was called to get it before
            cache.update(_.updateWith(webhook.id)(_.map(_ => webhook)))
        }
    }
}

object WebhooksProxy {
  type Env = Has[WebhookRepo] with Has[UpdateMode] with Clock

  val live: URLayer[Env, Has[WebhooksProxy]] =
    ZIO.services[WebhookRepo, UpdateMode].flatMap((start _).tupled).toLayer

  private def start(webhookRepo: WebhookRepo, updateMode: UpdateMode): URIO[Clock, WebhooksProxy] =
    for {
      cache <- Ref.make(Map.empty[WebhookId, Webhook])
      proxy  = WebhooksProxy(cache, webhookRepo, updateMode)
      _     <- proxy.start.fork
    } yield proxy

  sealed trait UpdateMode extends Product with Serializable
  object UpdateMode {
    final case class Polling(interval: Duration, f: PollingFunction) extends UpdateMode

    type PollingFunction = NonEmptySet[WebhookId] => UIO[Map[WebhookId, Webhook]]

    final case class Subscription(value: UStream[WebhookUpdate]) extends UpdateMode
  }
}
