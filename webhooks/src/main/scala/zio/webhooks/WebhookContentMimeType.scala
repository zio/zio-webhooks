package zio.webhooks

/**
 * A [[WebhookContentMimeType]] contains the
 * [[https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types MIME type]] that
 * should describe the content of a [[WebhookEvent]] or a [[zio.webhooks.internal.WebhookDispatch]]
 * which contains a non-empty set of `WebhookEvent`s.
 */
final case class WebhookContentMimeType(value: String)
