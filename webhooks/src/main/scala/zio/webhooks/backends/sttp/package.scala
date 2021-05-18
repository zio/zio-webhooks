package zio.webhooks.backends

import _root_.sttp.client.SttpBackend
import _root_.sttp.client.asynchttpclient.WebSocketHandler
import zio._
import zio.stream.Stream

package object sttp {
  /**
    * An [[SttpClient]] is a service that provides a ZIO [[SttpBackend]].
    * 
    * Taken from https://sttp.softwaremill.com/en/v2/backends/zio.html
    */
  type SttpClient = Has[SttpBackend[Task, Stream[Throwable, Byte], WebSocketHandler]]
}
