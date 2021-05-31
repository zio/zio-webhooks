package zio.webhooks

import zio.NonEmptyChunk

/**
 * A [[Dispatch]] represents a unit of delivery to a [[Webhook]]. It can have one or more
 * [[WebhookEvent]]s. Retries are done on dispatches since status updates on webhooks and their
 * respective events depend on the outcome of each dispatch.
 */
final case class Dispatch(webhook: Webhook, events: NonEmptyChunk[WebhookEvent])
