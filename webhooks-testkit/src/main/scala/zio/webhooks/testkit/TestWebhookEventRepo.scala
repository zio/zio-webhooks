package zio.webhooks.testkit

import zio._
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks.WebhookError._
import zio.webhooks._

trait TestWebhookEventRepo {
  def createEvent(event: WebhookEvent): UIO[Unit]

  def enqueueExisting: UIO[Unit]

  def subscribeToEvents: UManaged[Dequeue[WebhookEvent]]
}

object TestWebhookEventRepo {

  // Accessor Methods

  def createEvent(event: WebhookEvent): URIO[Has[TestWebhookEventRepo], Unit] =
    ZIO.serviceWith(_.createEvent(event))

  def enqueueExisting: URIO[Has[TestWebhookEventRepo], Unit] =
    ZIO.serviceWith(_.enqueueExisting)

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

  def enqueueExisting: UIO[Unit] =
    ref.get.flatMap(map => ZIO.foreach_(map.values)(hub.publish))

  def recoverEvents: UStream[WebhookEvent] =
    UStream.fromIterableM(ref.get.map(_.values.filter(_.isDelivering)))

  def setAllAsFailedByWebhookId(webhookId: WebhookId): IO[MissingEventsError, Unit] =
    for {
      updatedMap <- ref.modify { map =>
                      val allFailedByWebhookId =
                        for ((key, event) <- map if key.webhookId == webhookId)
                          yield (key, event.copy(status = WebhookEventStatus.Failed))
                      (allFailedByWebhookId, map ++ allFailedByWebhookId)
                    }
      _          <- hub.publishAll(updatedMap.values)
    } yield ()

  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): IO[MissingEventError, Unit] =
    for {
      maybeEvent <- ref.modify { map =>
                      map.get(key) match {
                        case None        =>
                          (None, map)
                        case Some(event) =>
                          val updatedEvent = event.copy(status = status)
                          (Some(updatedEvent), map.updated(key, updatedEvent))
                      }
                    }
      _          <- maybeEvent match {
                      case None        =>
                        ZIO.fail(MissingEventError(key))
                      case Some(event) =>
                        hub.publish(event).unit
                    }
    } yield ()

  def setEventStatusMany(
    keys: NonEmptySet[WebhookEventKey],
    status: WebhookEventStatus
  ): IO[MissingEventsError, Unit] =
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
                    ZIO.fail(MissingEventsError(missingKeys))
                  case Right(updatedEvents) =>
                    hub.publishAll(updatedEvents)
                }
    } yield ()

  def subscribeToEvents: UManaged[Dequeue[WebhookEvent]] =
    hub.subscribe

  def subscribeToNewEvents: UManaged[Dequeue[WebhookEvent]] =
    subscribeToEvents.map(_.filterOutput(_.isNew))
}
