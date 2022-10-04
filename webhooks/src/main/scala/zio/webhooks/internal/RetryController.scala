package zio.webhooks.internal

import zio._
import zio.prelude.NonEmptySet
import zio.stream.ZStream
import zio.webhooks._

import java.time.Instant

/**
 * The [[RetryController]] provides the entry point for retrying webhook events, and manages
 * dispatchers and state for each webhook.
 */
private[webhooks] final case class RetryController(
  private val clock: zio.Clock,
  private val config: WebhookServerConfig,
  private val errorHub: Hub[WebhookError],
  private val eventRepo: WebhookEventRepo,
  private val fatalPromise: Promise[Cause[Nothing], Nothing],
  private val httpClient: WebhookHttpClient,
  private val inputQueue: Queue[WebhookEvent],
  private val retryDispatchers: Ref.Synchronized[Map[WebhookId, RetryDispatcher]],
  private val retryStates: Ref.Synchronized[Map[WebhookId, RetryState]],
  private val serializePayload: SerializePayload,
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
                         ZIO.foreachDiscard(_) {
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
      now   <- clock.instant
      retry <- Queue.bounded[WebhookEvent](config.retry.capacity).map { retryQueue =>
                 (
                   RetryDispatcher(
                     clock,
                     config,
                     errorHub,
                     eventRepo,
                     fatalPromise,
                     httpClient,
                     retryStates,
                     retryQueue,
                     serializePayload,
                     shutdownSignal,
                     webhookId,
                     webhooksProxy
                   ),
                   RetryState(
                     now,
                     backoff = None,
                     failureCount = 0,
                     isActive = false,
                     now,
                     loadedState.timeLeft,
                     timerKillSwitch = None
                   )
                 )
               }
    } yield retry

  def persistRetries(timestamp: Instant): UIO[PersistentRetries] =
    suspendRetries(timestamp).map(map =>
      PersistentRetries(map.map {
        case (webhookId, retryState) =>
          val persistentRetry = PersistentRetries.PersistentRetryState(timeLeft = retryState.timeoutDuration)
          (webhookId.value, persistentRetry)
      })
    )

  def enqueueRetry(event: WebhookEvent): UIO[Any] =
    inputQueue.offer(event)

  def enqueueRetryMany(events: NonEmptySet[WebhookEvent]): UIO[Any] =
    inputQueue.offerAll(events)

  def start: UIO[Any] = {
    val handleRetryEvents =
      mergeShutdown(ZStream.fromQueue(inputQueue), shutdownSignal).tap(ZIO.succeed(_)).foreach { event =>
        val webhookId = event.key.webhookId
        for {
          retryQueue <- retryDispatchers.modifyZIO { map =>
                          map.get(webhookId) match {
                            case Some(dispatcher) =>
                              ZIO.succeed((dispatcher.retryQueue, map))
                            case None             =>
                              for {
                                retryQueue     <- Queue.bounded[WebhookEvent](config.retry.capacity)
                                now            <- clock.instant
                                retryDispatcher = RetryDispatcher(
                                                    clock,
                                                    config,
                                                    errorHub,
                                                    eventRepo,
                                                    fatalPromise,
                                                    httpClient,
                                                    retryStates,
                                                    retryQueue,
                                                    serializePayload,
                                                    shutdownSignal,
                                                    webhookId,
                                                    webhooksProxy
                                                  )
                                _              <- retryStates.updateZIO(states =>
                                                    ZIO.succeed(
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
          _          <- retryQueue.offer(event).race(shutdownSignal.await).unlessZIO(shutdownSignal.isDone)
        } yield ()
      }
    inputQueue.poll *> startupLatch.countDown *> handleRetryEvents
  }

  private def suspendRetries(timestamp: Instant): UIO[Map[WebhookId, RetryState]] =
    retryStates.get.map(_.map { case (id, retryState) => (id, retryState.suspend(timestamp)) })
}
