package zio.webhooks.internal

/**
 * A [[WebhookEventContentType]] is an HTTP content type supported by the [[zio.webhooks.WebhookServer]]. The
 * server uses this to indicate how the string contents of a batch of [[zio.webhooks.WebhookEvent]]s are put
 * together.
 *
 * Currently supported content types include:
 *   - plain text
 *   - JSON
 */
private[webhooks] sealed trait WebhookEventContentType
private[webhooks] object WebhookEventContentType {
  case object Json      extends WebhookEventContentType
  case object PlainText extends WebhookEventContentType
}
