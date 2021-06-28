package zio

import zio.json._
import zio.prelude.NonEmptySet

package object webhooks {

  private[webhooks] implicit class MapOps[K, V](self: Map[K, V]) {
    def updatedWithBackport[V1 >: V](key: K)(remappingFunction: Option[V] => Option[V1]): Map[K, V1] = {
      val previousValue = self.get(key)
      val nextValue     = remappingFunction(previousValue)
      (previousValue, nextValue) match {
        case (None, None)    => self
        case (Some(_), None) => self - key
        case (_, Some(v))    => self.updated(key, v)
      }
    }
  }

  private[webhooks] implicit def nonEmptySetDecoder[A: JsonDecoder]: JsonDecoder[NonEmptySet[A]] =
    JsonDecoder.set[A].mapOrFail(NonEmptySet.fromSetOption(_).toRight("Set is empty"))

  private[webhooks] implicit def nonEmptySetEncoder[A: JsonEncoder]: JsonEncoder[NonEmptySet[A]] =
    JsonEncoder.set[A].contramap(_.toSet)
}
