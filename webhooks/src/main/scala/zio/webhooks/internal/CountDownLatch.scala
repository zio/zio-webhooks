package zio.webhooks.internal

import zio._
import zio.stm._

private[webhooks] final class CountDownLatch private (count: TRef[Int]) {
  val countDown: UIO[Unit] =
    count.update(_ - 1).commit.unit

  val await: UIO[Unit] =
    count.get.retryUntil(_ <= 0).commit.unit
}

private[webhooks] object CountDownLatch {
  def make(count: Int): UIO[CountDownLatch] =
    TRef.make(count).map(r => new CountDownLatch(r)).commit
}
