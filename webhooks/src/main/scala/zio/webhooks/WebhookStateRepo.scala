package zio.webhooks

import zio.Task

/**
 * The webhooks state repo is used to store state necessary for the library to perform its
 * function. For ease of integration, this state is modeled as a string, and may be stored in a
 * VARCHAR in most databases.
 */
trait WebhookStateRepo {
  def getState: Task[String]

  def setState(state: String): Task[Unit]
}
