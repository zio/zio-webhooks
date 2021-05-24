package zio.webhooks

/**
 * A [[WebhookHttpResponse]] contains a `statusCode`: the only response data relevant to webhook
 * delivery.
 */
final class WebhookHttpResponse private (val statusCode: Int) extends AnyVal {
  def isSuccess: Boolean = statusCode == 200

  def isFailure: Boolean = !isSuccess
}

object WebhookHttpResponse {
  def apply(statusCode: Int): WebhookHttpResponse = new WebhookHttpResponse(statusCode)
}
