package zio.webhooks

import zio._
import zio.stream._
import java.time.Instant

/**
 * A [[Webhook]] represents a webhook (a web callback registered to receive notifications), and
 * contains an id, a URL, a label (used for diagnostic purposes), a status, and an optional secret
 * token, which is used for signing events.
 */
final case class Webhook(
  id: WebhookId,
  url: String,
  label: String,
  status: WebhookStatus,
  deliveryMode: WebhookDeliveryMode
)

final case class WebhookId(value: Long)

/**
 * A [[WebhookStatus]] describes the status of a webhook, which is either enabled or disabled, or
 * unavailable since a specified moment in time.
 */
sealed trait WebhookStatus
object WebhookStatus {
  case object Enabled                              extends WebhookStatus
  case object Disabled                             extends WebhookStatus
  final case class Retrying(sinceTime: Instant)    extends WebhookStatus
  final case class Unavailable(sinceTime: Instant) extends WebhookStatus
}

/**
 * A [[WebhookDeliveryMode]] represents the delivery mode for a webhook: either one event at a time,
 * or batched, in which case events may be batched together for communication efficiency.
 */
final case class WebhookDeliveryMode(batching: WebhookDeliveryBatching, semantics: WebhookDeliverySemantics)

sealed trait WebhookDeliveryBatching
object WebhookDeliveryBatching {
  case object Single  extends WebhookDeliveryBatching
  case object Batched extends WebhookDeliveryBatching
}

sealed trait WebhookDeliverySemantics
object WebhookDeliverySemantics {
  case object AtLeastOnce extends WebhookDeliverySemantics
  case object AtMostOnce  extends WebhookDeliverySemantics
}

/**
 * A [[WebhookRepo]] provides persistence facilities for webhooks.
 */
trait WebhookRepo {
  def getWebhookById(webhookId: WebhookId): Task[Option[Webhook]]

  def setWebhookStatus(id: WebhookId, status: WebhookStatus): Task[Unit]
}

/**
 * The webhooks state repo is used to store state necessary for the library to perform its
 * function. For ease of integration, this state is modeled as a string, and may be stored in a
 * VARCHAR in most databases.
 */
trait WebhookStateRepo {
  def getState: Task[String]

  def setState(state: String): Task[Unit]
}

final case class WebhookKey(eventId: WebhookEventId, webhookId: WebhookId)

/**
 * A [[WebhookEvent]] stores the content of a webhook event.
 */
final case class WebhookEvent(
  key: WebhookKey,
  status: WebhookEventStatus,
  content: String,
  headers: Chunk[(String, String)]
)

final case class WebhookEventId(value: Long)

sealed trait WebhookEventStatus
object WebhookEventStatus {
  case object New        extends WebhookEventStatus
  case object Delivering extends WebhookEventStatus
  case object Delivered  extends WebhookEventStatus
  case object Failed     extends WebhookEventStatus
}

/**
 * A [[WebhookEventRepo]] provides persistence facilities for webhook events.
 */
trait WebhookEventRepo {

  /**
   * Retrieves all events by their status.
   *
   * TODO: Change return type to stream
   */
  def getEventsByStatus(statuses: NonEmptySet[WebhookEventStatus]): Stream[Throwable, WebhookEvent]

  def getEventsByWebhookAndStatus(
    id: WebhookId,
    status: NonEmptySet[WebhookEventStatus]
  ): Stream[Throwable, WebhookEvent]

  /**
   * Sets the status of the specified event.
   */
  def setEventStatus(key: WebhookKey, status: WebhookEventStatus): Task[Unit]

  /**
   * Marks all events by the specified webhook id as failed.
   */
  def setAllAsFailedByWebhookId(webhookId: WebhookId): Task[Unit]
}

final case class NonEmptySet[A](head: A, others: Set[A])
