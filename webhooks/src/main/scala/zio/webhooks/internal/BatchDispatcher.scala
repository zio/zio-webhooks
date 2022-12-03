package zio.webhooks.internal

//import zio.Cause.{Die, Fail}
import zio._
import zio.prelude.NonEmptySet
import zio.stream._
import zio.webhooks._

private[webhooks] final class BatchDispatcher private (
  private val batchingCapacity: Int,
  private val batchQueues: Ref.Synchronized[Map[BatchKey, Queue[WebhookEvent]]],
  private val deliver: (WebhookDispatch, Queue[WebhookEvent]) => UIO[Unit],
  private val fatalPromise: Promise[Cause[Nothing], Nothing],
  private val inputQueue: Queue[WebhookEvent],
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhooks: WebhooksProxy
) {

  private def deliverBatches(batchQueue: Queue[WebhookEvent], latch: Promise[Nothing, Unit]): UIO[Nothing] = {
    val deliverBatch = for {
      batch    <- batchQueue.take.zipWith(batchQueue.takeAll)(NonEmptySet.fromIterable(_, _))
      webhookId = batch.head.key.webhookId
      webhook  <- webhooks.getWebhookById(webhookId)
      dispatch  = WebhookDispatch(
                    webhook.id,
                    webhook.url,
                    webhook.deliveryMode.semantics,
                    WebhookPayload.Batched(batch)
                  )
      _        <- deliver(dispatch, batchQueue).when(webhook.isEnabled)
    } yield ()
    batchQueue.poll *> latch.succeed(()) *> deliverBatch.forever
  }

  def enqueueEvent(event: WebhookEvent): UIO[Unit] =
    inputQueue.offer(event).unit

  def start: UIO[Any] =
    mergeShutdown(ZStream.fromQueue(inputQueue), shutdownSignal).groupByKey { ev =>
      val (webhookId, contentType) = ev.webhookIdAndContentType
      BatchKey(webhookId, contentType)
    } {
      case (batchKey, events) =>
        ZStream.fromZIO(
          for {
            batchQueue <- batchQueues.modifyZIO { map =>
                            map.get(batchKey) match {
                              case Some(queue) =>
                                ZIO.succeed((queue, map))
                              case None        =>
                                for (queue <- Queue.bounded[WebhookEvent](batchingCapacity))
                                  yield (queue, map + (batchKey -> queue))
                            }
                          }
            latch      <- Promise.make[Nothing, Unit]
            _          <- deliverBatches(batchQueue, latch).onError(fatalPromise.fail).fork
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
    fatalPromise: Promise[Cause[Nothing], Nothing],
    shutdownSignal: Promise[Nothing, Unit],
    webhooks: WebhooksProxy
  ): UIO[BatchDispatcher] =
    for {
      batchQueue <- Ref.Synchronized.make(Map.empty[BatchKey, Queue[WebhookEvent]])
      inputQueue <- Queue.bounded[WebhookEvent](1)
      dispatcher  = new BatchDispatcher(
                      capacity,
                      batchQueue,
                      deliver,
                      fatalPromise,
                      inputQueue,
                      shutdownSignal,
                      webhooks
                    )
    } yield dispatcher
}
