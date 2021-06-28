package zio.webhooks

import zio._

/**
 * A [[WebhookStateRepo]] stores encoded server state, allowing retrying to suspend and continue
 * after the server restarts.
 *
 * For ease of integration, this state is modeled as a string.
 */
trait WebhookStateRepo {

  /**
   * Retrieves the encoded `String` state value.
   */
  def getState: UIO[Option[String]]

  /**
   * Sets the encoded `String` state value.
   */
  def setState(state: String): UIO[Unit]
}

object WebhookStateRepo {
  // accessors
  def getState: URIO[Has[WebhookStateRepo], Option[String]] =
    ZIO.serviceWith[WebhookStateRepo](_.getState)

  def setState(state: String): URIO[Has[WebhookStateRepo], Unit] =
    ZIO.serviceWith[WebhookStateRepo](_.setState(state))
}
