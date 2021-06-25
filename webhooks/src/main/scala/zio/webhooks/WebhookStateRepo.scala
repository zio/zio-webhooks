package zio.webhooks

import zio._

/**
 * A [[WebhookStateRepo]] is used to store state necessary for the server to perform its function.
 * For ease of integration, this state is modeled as a string, and may be stored in a `VARCHAR` in
 * most databases.
 */
trait WebhookStateRepo {

  /**
   * Retrieves the value of some `String` state.
   */
  def getState: UIO[Option[String]]

  /**
   * Sets the the value of some `String` state.
   */
  def setState(state: String): UIO[Unit]
}

object WebhookStateRepo {
  def getState: URIO[Has[WebhookStateRepo], Option[String]] =
    ZIO.serviceWith[WebhookStateRepo](_.getState)

  def setState(state: String): URIO[Has[WebhookStateRepo], Unit] =
    ZIO.serviceWith[WebhookStateRepo](_.setState(state))
}
