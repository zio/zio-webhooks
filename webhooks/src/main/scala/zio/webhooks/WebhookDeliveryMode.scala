package zio.webhooks

import java.time.Duration

/**
 * A [[WebhookDeliveryMode]] specifies two aspects of webhook delivery: [[WebhookDeliveryBatching]],
 * whether the delivery of events is done one-by-one or in batches; and
 * [[WebhookDeliverySemantics]], which specify a delivery goal of at least once, or at most once.
 */
final case class WebhookDeliveryMode private (batching: WebhookDeliveryBatching, semantics: WebhookDeliverySemantics)

object WebhookDeliveryMode {
  def batchedAtLeastOnce(size: Int, maxWait: Duration): WebhookDeliveryMode =
    WebhookDeliveryMode(WebhookDeliveryBatching.Batched(size, maxWait), WebhookDeliverySemantics.AtLeastOnce)

  def batchedAtMostOnce(size: Int, maxWait: Duration): WebhookDeliveryMode =
    WebhookDeliveryMode(WebhookDeliveryBatching.Batched(size, maxWait), WebhookDeliverySemantics.AtMostOnce)

  val SingleAtLeastOnce: WebhookDeliveryMode =
    WebhookDeliveryMode(WebhookDeliveryBatching.Single, WebhookDeliverySemantics.AtLeastOnce)
  val SingleAtMostOnce: WebhookDeliveryMode  =
    WebhookDeliveryMode(WebhookDeliveryBatching.Single, WebhookDeliverySemantics.AtMostOnce)
}
