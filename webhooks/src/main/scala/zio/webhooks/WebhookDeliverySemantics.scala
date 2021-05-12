package zio.webhooks

sealed trait WebhookDeliverySemantics
object WebhookDeliverySemantics {
  case object AtLeastOnce extends WebhookDeliverySemantics
  case object AtMostOnce  extends WebhookDeliverySemantics
}
