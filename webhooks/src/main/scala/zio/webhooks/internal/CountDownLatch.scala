package zio.webhooks.internal

import zio._
import zio.stm._

/**
 * A countdown latch used by [[zio.webhooks.WebhookServer]] to synchronize startup and shutdown.
 *
 * Taken mostly from [[https://fsvehla.blog/blog/2020/02/16/zio-stm-count-down-latch.html]].
 */
private[webhooks] final class CountDownLatch private (private val count: TRef[Int]) {
  val countDown: UIO[Unit] =
    count.update(_ - 1).commit.unit

  val await: UIO[Unit] =
    count.get.retryUntil(_ <= 0).commit.unit
}

private[webhooks] object CountDownLatch {
  def make(count: Int): UIO[CountDownLatch] =
    TRef.make(count).map(r => new CountDownLatch(r)).commit
}
