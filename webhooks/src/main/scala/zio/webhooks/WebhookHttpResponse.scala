package zio.webhooks

/**
 * A [[WebhookHttpResponse]] contains a `statusCode`, the only HTTP response data relevant to
 * webhook delivery.
 */
final case class WebhookHttpResponse(statusCode: Int)
