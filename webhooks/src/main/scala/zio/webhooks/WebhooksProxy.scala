package zio.webhooks

import zio._
import zio.webhooks.WebhookError.MissingWebhookError

/**
 * Mediates access to [[Webhook]]s, caching webhooks in memory while keeping them updated by polling
 * for some interval or with a subscription.
 */
final class WebhooksProxy private (
  private val webhookRepo: WebhookRepo,
  private val cache: Ref[Map[WebhookId, Webhook]]
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
}

object WebhooksProxy {
  val live: URLayer[Has[WebhookRepo], Has[WebhooksProxy]] =
    ZIO.serviceWith[WebhookRepo](make).toLayer

  private def make(webhookRepo: WebhookRepo): UIO[WebhooksProxy] =
    Ref.make(Map.empty[WebhookId, Webhook]).map(new WebhooksProxy(webhookRepo, _))
}