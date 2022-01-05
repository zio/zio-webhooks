package zio.webhooks

import zio.Chunk
import zio.prelude.NonEmptySet

sealed trait WebhookPayload {
  lazy val headers: Chunk[HttpHeader] =
    this match {
      case WebhookPayload.Single(event)   =>
        event.headers
      case WebhookPayload.Batched(events) =>
        events.head.headers
    }
}

object WebhookPayload {
  final case class Single(event: WebhookEvent)                extends WebhookPayload
  final case class Batched(events: NonEmptySet[WebhookEvent]) extends WebhookPayload {
    lazy val keys: NonEmptySet[WebhookEventKey] = NonEmptySet.fromSet(events.head.key, events.tail.map(_.key))
  }
}
