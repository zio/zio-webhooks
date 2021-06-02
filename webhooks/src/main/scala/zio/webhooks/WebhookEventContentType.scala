package zio.webhooks

/**
  * A [[WebhookEventContentType]] is an HTTP content type supported by the [[WebhookServer]]. The
  * server uses this to indicate how the string contents of a batch of [[WebhookEvent]]s are put
  * together.
  * 
  * Currently supported content types include:
  *   - plain text
  *   - JSON
  */
sealed trait WebhookEventContentType
object WebhookEventContentType {
  case object PlainText extends WebhookEventContentType
  case object Json      extends WebhookEventContentType
}
