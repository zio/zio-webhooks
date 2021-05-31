package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks.WebhookError._
import zio.webhooks._

trait TestWebhookEventRepo {
  def createEvent(event: WebhookEvent): UIO[Unit]

  def getEvents: UStream[WebhookEvent]
}

object TestWebhookEventRepo {
  // Layer Definitions

  val test: RLayer[Has[WebhookRepo], Has[WebhookEventRepo] with Has[TestWebhookEventRepo]] = {
    for {
      ref         <- Ref.makeManaged(Map.empty[WebhookEventKey, WebhookEvent])
      hub         <- Hub.unbounded[WebhookEvent].toManaged_
      webhookRepo <- ZManaged.service[WebhookRepo]
      impl         = TestWebhookEventRepoImpl(ref, hub, webhookRepo)
    } yield Has.allOf[WebhookEventRepo, TestWebhookEventRepo](impl, impl)
  }.toLayerMany

  // Accessor Methods

  def createEvent(event: WebhookEvent): URIO[Has[TestWebhookEventRepo], Unit] =
    ZIO.serviceWith(_.createEvent(event))

  def subscribeToEvents: ZStream[Has[TestWebhookEventRepo], Nothing, WebhookEvent] =
    for {
      eventRepo <- ZStream.environment[Has[TestWebhookEventRepo]].map(_.get)
      event     <- eventRepo.getEvents
    } yield event
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
                             val updatedEvent = event.copy(status = status)
                             (Some(updatedEvent), map.updated(key, updatedEvent))
                         }
                       }
      _             <- eventOpt match {
                         case None        =>
                           ZIO.fail(MissingWebhookEventError(key))
                         case Some(event) =>
                           hub.publish(event).unit
                       }
    } yield ()

  def getEvents: UStream[WebhookEvent] =
    Stream.fromHub(hub)

  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): UStream[WebhookEvent] =
    getEvents.filter(event => statuses.contains(event.status))

  def getEventsByWebhookAndStatus(
    id: WebhookId,
    statuses: NonEmptySet[WebhookEventStatus]
  ): Stream[WebhookError.MissingWebhookError, WebhookEvent] =
    getEventsByStatuses(statuses).filter(_.key.webhookId == id)
}
