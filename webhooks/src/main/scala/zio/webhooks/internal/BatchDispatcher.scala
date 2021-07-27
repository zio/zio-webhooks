package zio.webhooks.internal

import zio._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks._

private[webhooks] final class BatchDispatcher private (
  private val batchingCapacity: Int,
  private val batchQueues: RefM[Map[BatchKey, Queue[WebhookEvent]]],
  private val deliver: (WebhookDispatch, Queue[WebhookEvent]) => UIO[Unit],
  private val errorHub: Hub[WebhookError],
  private val inputQueue: Queue[WebhookEvent],
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhooks: WebhooksProxy
) {

  private def deliverBatches(batchQueue: Queue[WebhookEvent], latch: Promise[Nothing, Unit]): UIO[Nothing] = {
    val deliverBatch = for {
      batch    <- batchQueue.take.zipWith(batchQueue.takeAll)(NonEmptySet.fromIterable(_, _))
      webhookId = batch.head.key.webhookId
      webhook  <- webhooks.getWebhookById(webhookId)
      dispatch  = WebhookDispatch(webhook.id, webhook.url, webhook.deliveryMode.semantics, batch)
      _        <- deliver(dispatch, batchQueue).when(webhook.isEnabled)
    } yield ()
    batchQueue.poll *> latch.succeed(()) *> deliverBatch.catchAll(errorHub.publish(_)).forever
  }

  def enqueueEvent(event: WebhookEvent): UIO[Unit] =
    inputQueue.offer(event).unit

  def start: UIO[Any] =
    mergeShutdown(UStream.fromQueue(inputQueue), shutdownSignal).groupByKey { ev =>
      val (webhookId, contentType) = ev.webhookIdAndContentType
      BatchKey(webhookId, contentType)
    } {
      case (batchKey, events) =>
        ZStream.fromEffect(
          for {
            batchQueue <- batchQueues.modify { map =>
                            map.get(batchKey) match {
                              case Some(queue) =>
                                UIO((queue, map))
                              case None        =>
                                for (queue <- Queue.bounded[WebhookEvent](batchingCapacity))
                                  yield (queue, map + (batchKey -> queue))
                            }
                          }
            latch      <- Promise.make[Nothing, Unit]
            _          <- deliverBatches(batchQueue, latch).fork
            _          <- latch.await
            _          <- events.run(ZSink.fromQueue(batchQueue))
          } yield ()
        )
    }.runDrain
}

private[webhooks] object BatchDispatcher {
  def create(
    capacity: Int,
    deliver: (WebhookDispatch, Queue[WebhookEvent]) => UIO[Unit],
    errorHub: Hub[WebhookError],
    shutdownSignal: Promise[Nothing, Unit],
    webhooks: WebhooksProxy
  ): UIO[BatchDispatcher] =
    for {
      batchQueue <- RefM.make(Map.empty[BatchKey, Queue[WebhookEvent]])
      inputQueue <- Queue.bounded[WebhookEvent](1)
      dispatcher  = new BatchDispatcher(
                      capacity,
                      batchQueue,
                      deliver,
                      errorHub,
                      inputQueue,
                      shutdownSignal,
                      webhooks
                    )
    } yield dispatcher
}
