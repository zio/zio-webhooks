package zio.webhooks

/**
 * A [[WebhookHttpResponse]] contains a `statusCode`: the only response data relevant to webhook
 * delivery.
 */
final class WebhookHttpResponse(val statusCode: Int) extends AnyVal
