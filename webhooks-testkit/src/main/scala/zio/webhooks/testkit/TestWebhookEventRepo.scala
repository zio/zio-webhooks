package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream.{ UStream, ZStream }
import zio.webhooks._
import zio.webhooks.internal.DequeueUtils

import scala.util.control.NoStackTrace

trait TestWebhookEventRepo {
  def createEvent(event: WebhookEvent): UIO[Unit]

  def dumpEventIds: UIO[Set[(Long, WebhookEventStatus)]]

  def enqueueNew: UIO[Unit]

  def subscribeToEvents: URIO[Scope, Dequeue[WebhookEvent]]
}

object TestWebhookEventRepo {

  // Accessor Methods

  def createEvent(event: WebhookEvent): URIO[TestWebhookEventRepo, Unit] =
    ZIO.serviceWithZIO(_.createEvent(event))

  def dumpEventIds: URIO[TestWebhookEventRepo, Set[(Long, WebhookEventStatus)]] =
    ZIO.serviceWithZIO(_.dumpEventIds)

  def enqueueNew: URIO[TestWebhookEventRepo, Unit] =
    ZIO.serviceWithZIO(_.enqueueNew)

  def subscribeToEvents: URIO[Scope with TestWebhookEventRepo, Dequeue[WebhookEvent]] =
    ZIO.serviceWithZIO[TestWebhookEventRepo](_.subscribeToEvents)

  // Layer Definitions

  val test: ULayer[WebhookEventRepo with TestWebhookEventRepo] =
    ZLayer.fromZIO {
      for {
        ref <- Ref.make(Map.empty[WebhookEventKey, WebhookEvent])
        hub <- Hub.unbounded[WebhookEvent]
      } yield TestWebhookEventRepoImpl(ref, hub)
    }
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
    ref.get.flatMap(map => ZIO.foreachDiscard(map.values.filter(_.isNew))(hub.publish))

  def recoverEvents: UStream[WebhookEvent] =
    ZStream.fromIterableZIO(ref.get.map(_.values.filter(_.isDelivering)))

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

  def subscribeToEvents: URIO[Scope, Dequeue[WebhookEvent]] =
    hub.subscribe

  def subscribeToNewEvents: URIO[Scope, Dequeue[WebhookEvent]] =
    subscribeToEvents.map(d => DequeueUtils.filterOutput[WebhookEvent](d, _.isNew))
}
