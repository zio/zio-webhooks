package zio

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
}
