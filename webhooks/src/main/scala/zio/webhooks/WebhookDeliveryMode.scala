package zio.webhooks

/**
 * A [[WebhookDeliveryMode]] represents the delivery mode for a webhook: either one event at a time,
 * or batched, in which case events may be batched together for communication efficiency.
 */
final case class WebhookDeliveryMode(batching: WebhookDeliveryBatching, semantics: WebhookDeliverySemantics)
