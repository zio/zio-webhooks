package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks._

import scala.util.control.NoStackTrace

trait TestWebhookEventRepo {
  def createEvent(event: WebhookEvent): UIO[Unit]

  def dumpEventIds: UIO[Set[(Long, WebhookEventStatus)]]

  def enqueueNew: UIO[Unit]

  def subscribeToEvents: UManaged[Dequeue[WebhookEvent]]
}

object TestWebhookEventRepo {

  // Accessor Methods

  def createEvent(event: WebhookEvent): URIO[Has[TestWebhookEventRepo], Unit] =
    ZIO.serviceWith(_.createEvent(event))

  def dumpEventIds: URIO[Has[TestWebhookEventRepo], Set[(Long, WebhookEventStatus)]] =
    ZIO.serviceWith(_.dumpEventIds)

  def enqueueNew: URIO[Has[TestWebhookEventRepo], Unit] =
    ZIO.serviceWith(_.enqueueNew)

  def subscribeToEvents: URManaged[Has[TestWebhookEventRepo], Dequeue[WebhookEvent]] =
    ZManaged.service[TestWebhookEventRepo].flatMap(_.subscribeToEvents)

  // Layer Definitions

  val test: ULayer[Has[WebhookEventRepo] with Has[TestWebhookEventRepo]] = {
    for {
      ref <- Ref.make(Map.empty[WebhookEventKey, WebhookEvent])
      hub <- Hub.unbounded[WebhookEvent]
      impl = TestWebhookEventRepoImpl(ref, hub)
    } yield Has.allOf[WebhookEventRepo, TestWebhookEventRepo](impl, impl)
  }.toLayerMany
}

final private case class TestWebhookEventRepoImpl(
  ref: Ref[Map[WebhookEventKey, WebhookEvent]],
  hub: Hub[WebhookEvent]
) extends WebhookEventRepo
    with TestWebhookEventRepo {

  def createEvent(event: WebhookEvent): UIO[Unit] =
    ref.update(_.updated(event.key, event)) <* hub.publish(event)

  def dumpEventIds: UIO[Set[(Long, WebhookEventStatus)]] =
    ref.get.map(_.map { case (key, ev) => (key.eventId.value, ev.status) }.toSet)

  def enqueueNew: UIO[Unit]                              =
    ref.get.flatMap(map => ZIO.foreach_(map.values.filter(_.isNew))(hub.publish))

  def recoverEvents: UStream[WebhookEvent] =
    UStream.fromIterableM(ref.get.map(_.values.filter(_.isDelivering)))

  def setAllAsFailedByWebhookId(webhookId: WebhookId): UIO[Unit] =
    for {
      updatedMap <- ref.modify { map =>
                      val allFailedByWebhookId =
                        for ((key, event) <- map if key.webhookId == webhookId)
                          yield (key, event.copy(status = WebhookEventStatus.Failed))
                      (allFailedByWebhookId, map ++ allFailedByWebhookId)
                    }
      _          <- hub.publishAll(updatedMap.values)
    } yield ()

  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): UIO[Unit] =
    for {
      event <- ref.modify { map =>
                 val event        = map(key)
                 val updatedEvent = event.copy(status = status)
                 (updatedEvent, map.updated(key, updatedEvent))
               }
      _     <- hub.publish(event).unit
    } yield ()

  def setEventStatusMany(keys: NonEmptySet[WebhookEventKey], status: WebhookEventStatus): UIO[Unit] =
    for {
      result <- ref.modify { map =>
                  val missingKeys = keys.filter(!map.contains(_))
                  if (missingKeys.nonEmpty)
                    (NonEmptySet.fromSetOption(missingKeys).toLeft(Iterable.empty[WebhookEvent]), map)
                  else {
                    val updated =
                      for ((key, event) <- map if keys.contains(key))
                        yield (key, event.copy(status = status))
                    (Right(updated.values), if (status.isDone) map -- keys else map ++ updated)
                  }
                }
      _      <- result match {
                  case Left(missingKeys)    =>
                    throw new NoStackTrace { override def getMessage = s"Fatal error, missing keys: $missingKeys" }
                  case Right(updatedEvents) =>
                    hub.publishAll(updatedEvents)
                }
    } yield ()

  def subscribeToEvents: UManaged[Dequeue[WebhookEvent]] =
    hub.subscribe

  def subscribeToNewEvents: UManaged[Dequeue[WebhookEvent]] =
    subscribeToEvents.map(_.filterOutput(_.isNew))
}
