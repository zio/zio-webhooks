package zio.webhooks

/**
 * A [[WebhookDeliveryMode]] specifies two aspects of webhook delivery: [[WebhookDeliveryBatching]], whether the
 * delivery of events is done one-by-one or in batches; and [[WebhookDeliverySemantics]], which specify a delivery goal
 * of at least once, or at most once.
 */
final case class WebhookDeliveryMode(batching: WebhookDeliveryBatching, semantics: WebhookDeliverySemantics)
