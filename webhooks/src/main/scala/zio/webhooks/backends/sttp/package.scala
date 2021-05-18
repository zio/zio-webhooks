package zio.webhooks.backends

import _root_.sttp.client.SttpBackend
import _root_.sttp.client.asynchttpclient.WebSocketHandler
import zio._
import zio.stream.Stream

package object sttp {

  /**
   * An [[SttpClient]] is an `SttpBackend` for ZIO.
   *
   * Taken from https://sttp.softwaremill.com/en/v2/backends/zio.html
   */
  type SttpClient = SttpBackend[Task, Stream[Throwable, Byte], WebSocketHandler]
}
