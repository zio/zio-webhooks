package zio.webhooks

/**
 * A [[WebhookHttpResponse]] contains a `statusCode`: the only response data relevant to webhook
 * delivery.
 */
final class WebhookHttpResponse private (val statusCode: Int) extends AnyVal

object WebhookHttpResponse {
  def apply(statusCode: Int) = new WebhookHttpResponse(statusCode)
}
