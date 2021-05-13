package zio.webhooks

/**
 * [[WebhookDeliverySemantics]] specify the delivery goal of a [[Webhook]]. If we deliver `AtLeastOnce`, we have to
 * ensure reliable delivery of a [[WebhookEvent]] by retrying even though it may result in message duplication.
 * `AtMostOnce` indicates delivery can fail, but cannot be done more than once.
 */
sealed trait WebhookDeliverySemantics
object WebhookDeliverySemantics {
  case object AtLeastOnce extends WebhookDeliverySemantics
  case object AtMostOnce  extends WebhookDeliverySemantics
}
