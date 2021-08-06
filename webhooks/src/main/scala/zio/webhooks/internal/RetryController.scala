package zio.webhooks.internal

import zio._
import zio.prelude.NonEmptySet
import zio.stream.UStream
import zio.webhooks._

import java.time.Instant

/**
 * The [[RetryController]] provides the entry point for retrying webhook events, and manages
 * dispatchers and state for each webhook.
 */
private[webhooks] final case class RetryController(
  private val clock: zio.clock.Clock.Service,
  private val config: WebhookServerConfig,
  private val errorHub: Hub[WebhookError],
  private val eventRepo: WebhookEventRepo,
  private val httpClient: WebhookHttpClient,
  private val inputQueue: Queue[WebhookEvent],
  private val retryDispatchers: RefM[Map[WebhookId, RetryDispatcher]],
  private val retryStates: RefM[Map[WebhookId, RetryState]],
  private val shutdownLatch: CountDownLatch,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val startupLatch: CountDownLatch,
  private val webhooksProxy: WebhooksProxy
) {
  def isActive(webhookId: WebhookId): UIO[Boolean] =
    retryStates.get.map(_.get(webhookId).exists(_.isActive))

  def loadRetries(persistentRetries: PersistentRetries): IO[WebhookError, Unit] =
    for {
      loadedRetries <- ZIO.foreach(persistentRetries.retryStates) {
                         case (id, loadedState) =>
                           loadRetry(WebhookId(id), loadedState).map((WebhookId(id), _))
                       }
      _             <- retryDispatchers.set(loadedRetries.map { case (id, (dispatcher, _)) => (id, dispatcher) })
      _             <- retryStates.set(loadedRetries.map { case (id, (_, retryState)) => (id, retryState) })
      _             <- retryDispatchers.get.flatMap {
                         ZIO.foreach_(_) {
                           case (_, retryDispatcher) =>
                             retryDispatcher.start *> retryDispatcher.activateWithTimeout
                         }
                       }
    } yield ()

  /**
   * Resumes retry delivery for a webhook given a persisted retry state loaded on startup.
   */
  private def loadRetry(
    webhookId: WebhookId,
    loadedState: PersistentRetries.PersistentRetryState
  ): IO[WebhookError, (RetryDispatcher, RetryState)] =
    for {
      now <- clock.instant
      foo <- Queue.bounded[WebhookEvent](config.retry.capacity).map { retryQueue =>
               (
                 RetryDispatcher(
                   clock,
                   config,
                   errorHub,
                   eventRepo,
                   httpClient,
                   retryStates,
                   retryQueue,
                   shutdownSignal,
                   webhookId,
                   webhooksProxy
                 ),
                 RetryState(
                   now,
                   loadedState.backoff,
                   loadedState.failureCount,
                   isActive = false,
                   now,
                   loadedState.timeLeft,
                   timerKillSwitch = None
                 )
               )
             }
    } yield foo

  def persistRetries(timestamp: Instant): UIO[PersistentRetries] =
    suspendRetries(timestamp).map(map =>
      PersistentRetries(map.map {
        case (webhookId, retryState) =>
          val persistentRetry = PersistentRetries.PersistentRetryState(
            backoff = retryState.backoff,
            failureCount = retryState.failureCount,
            timeLeft = retryState.timeoutDuration
          )
          (webhookId.value, persistentRetry)
      })
    )

  def enqueueRetry(event: WebhookEvent): UIO[Any] =
    inputQueue.offer(event)

  def enqueueRetryMany(events: NonEmptySet[WebhookEvent]): UIO[Any] =
    inputQueue.offerAll(events)

  def start: UIO[Any] = {
    val handleRetryEvents = mergeShutdown(UStream.fromQueue(inputQueue), shutdownSignal).foreach { event =>
      val webhookId = event.key.webhookId
      for {
        retryQueue <- retryDispatchers.modify { map =>
                        map.get(webhookId) match {
                          case Some(dispatcher) =>
                            UIO((dispatcher.retryQueue, map))
                          case None             =>
                            for {
                              retryQueue     <- Queue.bounded[WebhookEvent](config.retry.capacity)
                              now            <- clock.instant
                              retryDispatcher = RetryDispatcher(
                                                  clock,
                                                  config,
                                                  errorHub,
                                                  eventRepo,
                                                  httpClient,
                                                  retryStates,
                                                  retryQueue,
                                                  shutdownSignal,
                                                  webhookId,
                                                  webhooksProxy
                                                )
                              _              <- retryStates.update(states =>
                                                  UIO(
                                                    states + (webhookId -> RetryState(
                                                      activeSinceTime = now,
                                                      backoff = None,
                                                      failureCount = 0,
                                                      isActive = false,
                                                      lastRetryTime = now,
                                                      timeoutDuration = config.retry.timeout,
                                                      timerKillSwitch = None
                                                    ))
                                                  )
                                                )
                              _              <- retryDispatcher.start
                            } yield (retryQueue, map + (webhookId -> retryDispatcher))
                        }
                      }
        isShutDown <- shutdownSignal.isDone
        _          <- retryQueue.offer(event).race(shutdownSignal.await).unless(isShutDown)
      } yield ()
    } *> shutdownLatch.countDown
    inputQueue.poll *> startupLatch.countDown *> handleRetryEvents.fork
  }

  private def suspendRetries(timestamp: Instant): UIO[Map[WebhookId, RetryState]] =
    retryStates.get.map(_.map { case (id, retryState) => (id, retryState.suspend(timestamp)) })
}
