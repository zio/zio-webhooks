package zio

import zio.stream.UStream

package object webhooks {

  private[webhooks] implicit class MapOps[K, V](self: Map[K, V]) {

    /**
     * A backport of Scala 2.13's [[scala.collection.immutable.MapOps.removedAll]]
     */
    def removeAll(keys: Iterable[K]): Map[K, V] =
      keys.iterator.foldLeft(self)(_ - _)

    /**
     * A backport of Scala 2.13's [[scala.collection.immutable.MapOps.updatedWith]]
     */
    def updateWith[V1 >: V](key: K)(remappingFunction: Option[V] => Option[V1]): Map[K, V1] = {
      val previousValue = self.get(key)
      val nextValue     = remappingFunction(previousValue)
      (previousValue, nextValue) match {
        case (None, None)    => self
        case (Some(_), None) => self - key
        case (_, Some(v))    => self.updated(key, v)
      }
    }
  }

  private[webhooks] def mergeShutdown[A](stream: UStream[A], shutdownSignal: Promise[Nothing, Unit]): UStream[A] =
    stream
      .map(Left(_))
      .mergeTerminateRight(UStream.fromEffect(shutdownSignal.await.map(Right(_))))
      .collectLeft

  /**
   * [[SerializePayload]] is a function that takes a [[WebhookPayload]] and an optional
   * [WebhookContentMimeType] and serializes it into a `String`.
   */
  type SerializePayload = (WebhookPayload, Option[WebhookContentMimeType]) => String
}
