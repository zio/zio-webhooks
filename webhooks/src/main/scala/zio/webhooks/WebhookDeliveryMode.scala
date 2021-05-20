package zio.webhooks

/**
 * A [[WebhookDeliveryMode]] specifies two aspects of webhook delivery: [[WebhookDeliveryBatching]],
 * whether the delivery of events is done one-by-one or in batches; and
 * [[WebhookDeliverySemantics]], which specify a delivery goal of at least once, or at most once.
 */
final case class WebhookDeliveryMode private (batching: WebhookDeliveryBatching, semantics: WebhookDeliverySemantics)

object WebhookDeliveryMode {
  val BatchedAtLeastOnce = WebhookDeliveryMode(WebhookDeliveryBatching.Batched, WebhookDeliverySemantics.AtLeastOnce)
  val BatchedAtMostOnce  = WebhookDeliveryMode(WebhookDeliveryBatching.Batched, WebhookDeliverySemantics.AtMostOnce)
  val SingleAtLeastOnce  = WebhookDeliveryMode(WebhookDeliveryBatching.Single, WebhookDeliverySemantics.AtLeastOnce)
  val SingleAtMostOnce   = WebhookDeliveryMode(WebhookDeliveryBatching.Single, WebhookDeliverySemantics.AtMostOnce)
}
