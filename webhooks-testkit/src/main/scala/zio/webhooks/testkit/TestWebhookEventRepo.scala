package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks._
import zio.webhooks.WebhookError._

trait TestWebhookEventRepo {
  def createEvent(event: WebhookEvent): UIO[Unit]
}

object TestWebhookEventRepo {
  val testLayer
    : RLayer[Has[WebhookRepo], Has[WebhookEventRepo] with Has[TestWebhookEventRepo] with Has[WebhookRepo]] = {
    for {
      ref         <- Ref.make(Map.empty[WebhookEventKey, WebhookEvent])
      hub         <- Hub.unbounded[WebhookEvent]
      webhookRepo <- ZIO.service[WebhookRepo]
      impl         = TestWebhookEventRepoImpl(ref, hub, webhookRepo)
    } yield Has.allOf[WebhookEventRepo, TestWebhookEventRepo, WebhookRepo](impl, impl, webhookRepo)
  }.toLayerMany
}

final private case class TestWebhookEventRepoImpl(
  ref: Ref[Map[WebhookEventKey, WebhookEvent]],
  hub: Hub[WebhookEvent],
  webhookRepo: WebhookRepo
) extends WebhookEventRepo
    with TestWebhookEventRepo {

  def createEvent(event: WebhookEvent): UIO[Unit] =
    for {
      _ <- ref.update(_.updated(event.key, event))
      _ <- hub.publish(event)
    } yield ()

  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): UStream[WebhookEvent] =
    Stream.fromHub(hub).filter(event => statuses.contains(event.status))

  def getEventsByWebhookAndStatus(
    id: WebhookId,
    statuses: NonEmptySet[WebhookEventStatus]
  ): Stream[WebhookError.MissingWebhookError, WebhookEvent] =
    getEventsByStatuses(statuses).filter(_.key.webhookId == id)

  def setAllAsFailedByWebhookId(webhookId: WebhookId): IO[MissingWebhookError, Unit] =
    for {
      webhookExists <- webhookRepo.getWebhookById(webhookId).map(_.isDefined)
      _             <- ZIO.unless(webhookExists)(ZIO.fail(MissingWebhookError(webhookId)))
      updatedMap    <- ref.updateAndGet { map =>
                         map ++ (
                           for ((key, event) <- map if (key.webhookId == webhookId))
                             yield (key, event.copy(status = WebhookEventStatus.Failed))
                         )
                       }
      _             <- hub.publishAll(updatedMap.values)
    } yield ()

  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): IO[WebhookError, Unit] =
    for {
      webhookExists <- webhookRepo.getWebhookById(key.webhookId).map(_.isDefined)
      _             <- ZIO.unless(webhookExists)(ZIO.fail(MissingWebhookError(key.webhookId)))
      eventOpt      <- ref.modify { map =>
                         map.get(key) match {
                           case None        =>
                             (None, map)
                           case Some(event) =>
                             (Some(event), map.updated(key, event.copy(status = status)))
                         }
                       }
      _             <- eventOpt.fold[IO[MissingWebhookEventError, Unit]](
                         ZIO.fail(MissingWebhookEventError(key))
                       )(event => hub.publish(event).unit)
    } yield ()
}
