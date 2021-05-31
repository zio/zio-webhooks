package zio.webhooks

/**
 * A [[WebhookHttpResponse]] contains a `statusCode`: the only response data relevant to webhook
 * delivery.
 */
final case class WebhookHttpResponse(statusCode: Int) {
  def isSuccess: Boolean = statusCode == 200

  def isFailure: Boolean = !isSuccess
}
