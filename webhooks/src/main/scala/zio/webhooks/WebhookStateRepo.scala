package zio.webhooks

import zio.Task

/**
 * A [[WebhookStateRepo]] is used to store state necessary for the library to perform its
 * function. For ease of integration, this state is modeled as a string, and may be stored in a
 * `VARCHAR` in most databases.
 */
trait WebhookStateRepo {

  /**
   * Retrieves the value of some `String` state.
   */
  def getState: Task[String]

  /**
   * Sets the the value of some `String` state.
   */
  def setState(state: String): Task[Unit]
}
