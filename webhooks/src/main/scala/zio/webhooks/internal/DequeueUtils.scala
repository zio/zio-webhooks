package zio.webhooks.internal
import zio.{ Chunk, Dequeue, Trace, UIO, ZIO }

import scala.collection.mutable.ListBuffer

private[webhooks] object DequeueUtils {

  final def filterOutput[A](d: Dequeue[A], f: A => Boolean): Dequeue[A] =
    new Dequeue[A] {
      override def awaitShutdown(implicit trace: Trace): UIO[Unit] = d.awaitShutdown

      override def capacity: Int = d.capacity

      override def isShutdown(implicit trace: Trace): UIO[Boolean] = d.isShutdown

      override def shutdown(implicit trace: Trace): UIO[Unit] = d.shutdown

      override def size(implicit trace: Trace): UIO[Int] = d.size

      override def take(implicit trace: Trace): UIO[A] =
        d.take.flatMap { b =>
          if (f(b)) ZIO.succeedNow(b)
          else take
        }

      override def takeAll(implicit trace: Trace): UIO[Chunk[A]] =
        d.takeAll.map(bs => bs.filter(f))

      override def takeUpTo(max: Int)(implicit trace: Trace): UIO[Chunk[A]] =
        ZIO.suspendSucceed {
          val buffer                    = ListBuffer[A]()
          def loop(max: Int): UIO[Unit] =
            d.takeUpTo(max).flatMap { bs =>
              if (bs.isEmpty) ZIO.unit
              else {
                val filtered = bs.filter(f)

                buffer ++= filtered

                val length = filtered.length
                if (length == max) ZIO.unit
                else loop(max - length)
              }
            }
          loop(max).as(Chunk.fromIterable(buffer))
        }
    }

  final def map[A, B](d: Dequeue[A], f: A => B): Dequeue[B] =
    new Dequeue[B] {
      override def awaitShutdown(implicit trace: Trace): UIO[Unit] = d.awaitShutdown

      override def capacity: Int = d.capacity

      override def isShutdown(implicit trace: Trace): UIO[Boolean] = d.isShutdown

      override def shutdown(implicit trace: Trace): UIO[Unit] = d.shutdown

      override def size(implicit trace: Trace): UIO[Int] = d.size

      override def take(implicit trace: Trace): UIO[B] = d.take.map(f)

      override def takeAll(implicit trace: Trace): UIO[Chunk[B]] = d.takeAll.map(_.map(f))

      override def takeUpTo(max: Int)(implicit trace: Trace): UIO[Chunk[B]] = d.takeUpTo(max).map(_.map(f))
    }

  implicit final class DequeueOps[A](d: Dequeue[A]) {
    final def filterOutput(f: A => Boolean): Dequeue[A] =
      DequeueUtils.filterOutput(d, f)

    final def map[B](f: A => B): Dequeue[B] =
      DequeueUtils.map(d, f)
  }
}
