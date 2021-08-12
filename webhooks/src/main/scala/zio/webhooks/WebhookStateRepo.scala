package zio.webhooks

import zio._

/**
 * A [[WebhookStateRepo]] stores encoded server state, allowing the [[WebhookServer]] state to
 * suspend and continue after restarting.
 *
 * For ease of integration, this state is modeled as a string.
 */
trait WebhookStateRepo {

  /**
   * Retrieves then clears the encoded `String` state value.
   */
  def loadState: UIO[Option[String]]

  /**
   * Sets the encoded `String` state value.
   */
  def setState(state: String): UIO[Unit]
}

object WebhookStateRepo {
  // accessors
  def loadState: URIO[Has[WebhookStateRepo], Option[String]] =
    ZIO.serviceWith[WebhookStateRepo](_.loadState)

  def saveState(state: String): URIO[Has[WebhookStateRepo], Unit] =
    ZIO.serviceWith[WebhookStateRepo](_.setState(state))
}
