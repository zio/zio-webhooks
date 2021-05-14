package zio.webhooks

import zio.{ Has, ULayer }
import zio.Task
import zio.prelude.NonEmptySet

final case class TestWebhookEventRepo() extends WebhookEventRepo {

  def getEventsByStatuses(statuses: NonEmptySet[WebhookEventStatus]): zio.stream.Stream[Throwable, WebhookEvent] =
    ???

  def getEventsByWebhookAndStatus(
    id: WebhookId,
    status: NonEmptySet[WebhookEventStatus]
  ): zio.stream.Stream[Throwable, WebhookEvent] =
    ???

  def setEventStatus(key: WebhookEventKey, status: WebhookEventStatus): Task[Unit] =
    ???

  def setAllAsFailedByWebhookId(webhookId: WebhookId): Task[Unit] = ???
}

object TestWebhookEventRepo {
  val testLayer: ULayer[Has[WebhookEventRepo]] = ???
}
