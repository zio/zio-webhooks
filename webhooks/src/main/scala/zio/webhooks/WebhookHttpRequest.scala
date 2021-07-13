package zio.webhooks

import zio.Chunk

/**
 * A [[WebhookHttpRequest]] contains a subset of an HTTP request required to send webhook data.
 */
final case class WebhookHttpRequest private[webhooks] (url: String, content: String, headers: Chunk[(String, String)])

object WebhookHttpRequest {
  private[webhooks] def fromDispatch(dispatch: WebhookDispatch): WebhookHttpRequest = {
    val requestContent =
      dispatch.contentType match {
        case Some(WebhookEventContentType.Json) =>
          if (dispatch.size > 1)
            "[" + dispatch.events.map(_.content).mkString(",") + "]"
          else
            dispatch.events.head.content
        case _                                  =>
          dispatch.events.map(_.content).mkString
      }
    WebhookHttpRequest(dispatch.url, requestContent, dispatch.headers)
  }
}
