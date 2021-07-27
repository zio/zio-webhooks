package zio.webhooks
import zio.webhooks.WebhookEventStatus._

/**
 * A [[WebhookEventStatus]] denotes the lifecycle of a [[WebhookEvent]]. Upon creation, an event is
 * `New`. On [[WebhookServer]] startup, events that are loaded, reloaded, or being retried are
 * marked `Delivering`. Once the server gets back a success from the webhook URL, the event is
 * marked `Delivered`. If after 7 days our webhook fails to delivery any events, we mark all events
 * under that webhook `Failed`.
 */
sealed trait WebhookEventStatus extends Product with Serializable {
  final def isDone: Boolean = !isPending

  final def isPending: Boolean =
    this match {
      case New        => true
      case Delivering => true
      case Delivered  => false
      case Failed     => false
    }
}

object WebhookEventStatus {
  // TODO: document meanings
  case object New        extends WebhookEventStatus
  case object Delivering extends WebhookEventStatus
  case object Delivered  extends WebhookEventStatus
  case object Failed     extends WebhookEventStatus
}
