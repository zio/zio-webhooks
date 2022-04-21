package zio.webhooks.internal

import zio._
import zio.Clock
import zio.webhooks._

/**
 * A [[RetryDispatcher]] performs retry delivery for a single webhook.
 */
private[webhooks] final case class RetryDispatcher(
  private val clock: Clock,
  private val config: WebhookServerConfig,
  private val errorHub: Hub[WebhookError],
  private val eventRepo: WebhookEventRepo,
  private val fatalPromise: Promise[Cause[Nothing], Nothing],
  private val httpClient: WebhookHttpClient,
  private val retryStates: Ref.Synchronized[Map[WebhookId, RetryState]],
  retryQueue: Queue[WebhookEvent],
  private val serializePayload: SerializePayload,
  private val shutdownSignal: Promise[Nothing, Unit],
  private val webhookId: WebhookId,
  private val webhooksProxy: WebhooksProxy
) {

  /**
   * Activates a timer that marks a webhook unavailable should the retry state remain active past
   * the timeout duration. The timer is killed when retrying is deactivated.
   */
  private[internal] def activateWithTimeout: UIO[Unit] =
    retryStates.updateZIO { retryStates =>
      val currentState = retryStates(webhookId)
      for {
        nextState <- if (currentState.isActive)
                       UIO.succeed(currentState)
                     else
                       for {
                         timerKillSwitch <- Promise.make[Nothing, Unit]
                         runTimer         = timerKillSwitch.await
                                              .timeoutTo(false)(_ => true)(currentState.timeoutDuration)
                                              .flatMap(markWebhookUnavailable(webhookId).unless(_))
                         _               <- runTimer.fork.provideLayer(ZLayer.succeed(clock))
                       } yield currentState.copy(isActive = true, timerKillSwitch = Some(timerKillSwitch))
      } yield retryStates.updated(webhookId, nextState)
    }

  private def markDispatch(dispatch: WebhookDispatch, newStatus: WebhookEventStatus): IO[WebhookError, Unit] =
    dispatch.payload match {
      case WebhookPayload.Single(event)      =>
        eventRepo.setEventStatus(event.key, newStatus)
      case batch @ WebhookPayload.Batched(_) =>
        eventRepo.setEventStatusMany(batch.keys, newStatus)
    }

  /**
   * Marks a webhook unavailable, marking all its events failed.
   */
  private def markWebhookUnavailable(webhookId: WebhookId): IO[WebhookError, Unit] =
    for {
      _                 <- eventRepo.setAllAsFailedByWebhookId(webhookId)
      unavailableStatus <- clock.instant.map(WebhookStatus.Unavailable)
      _                 <- webhooksProxy.setWebhookStatus(webhookId, unavailableStatus)
    } yield ()

  /**
   * Attempts to retry a dispatch. Each attempt updates the retry state based on
   * the outcome of the attempt.
   *
   * Each failed attempt causes the retry backoff to increase exponentially, so as not to flood the
   * endpoint with retry attempts.
   *
   * On the other hand, each successful attempt resets backoff—allowing for greater throughput
   * for retries when the endpoint begins to return `200` status codes.
   */
  private def retryEvents(dispatch: WebhookDispatch, batchQueue: Option[Queue[WebhookEvent]]): UIO[Unit] = {
    val (payload, headers) = serializePayload(dispatch.payload, dispatch.contentType)
    val request            = WebhookHttpRequest(dispatch.url, payload, dispatch.headers ++ headers)
    for {
      response <- httpClient.post(request).either
      _        <- response match {
                    case Left(Left(badWebhookUrlError))  =>
                      errorHub.publish(badWebhookUrlError)
                    case Right(WebhookHttpResponse(200)) =>
                      for {
                        _   <- markDispatch(dispatch, WebhookEventStatus.Delivered)
                        now <- clock.instant
                        _   <- retryStates.updateZIO { retryStates =>
                                 val newState = retryStates(dispatch.webhookId).resetBackoff(now)
                                 for {
                                   queueEmpty       <- retryQueue.size.map(_ <= 0)
                                   batchExistsEmpty <- ZIO
                                                         .foreach(batchQueue)(_.size.map(_ <= 0))
                                                         .map(_.getOrElse(true))
                                   allEmpty          = queueEmpty && batchExistsEmpty
                                   newState         <- if (allEmpty) newState.deactivate else UIO.succeed(newState)
                                 } yield retryStates.updated(dispatch.webhookId, newState)
                               }
                      } yield ()
                    // retry responded with a non-200 status, or an IOException occurred
                    // move the retry state to the next backoff duration
                    case _                               =>
                      for {
                        timestamp <- clock.instant
                        _         <- retryStates.updateZIO { retryStates =>
                                       retryQueue.offerAll(dispatch.events).fork *>
                                         UIO.succeed(
                                           retryStates.updated(
                                             webhookId,
                                             retryStates(webhookId).increaseBackoff(timestamp, config.retry)
                                           )
                                         )
                                     }
                      } yield ()
                  }
    } yield ()
  }.catchAll(errorHub.publish(_).unit)

  def start: UIO[Any] = {
    val deliverFunc =
      (dispatch: WebhookDispatch, batchQueue: Queue[WebhookEvent]) => retryEvents(dispatch, Some(batchQueue))
    for {
      batchDispatcher <- ZIO.foreach(config.batchingCapacity)(
                           BatchDispatcher
                             .create(_, deliverFunc, fatalPromise, shutdownSignal, webhooksProxy)
                             .tap(_.start.fork)
                         )
      handleEvent      = for {
                           _     <- ZIO.unit
                           take   = retryStates.get.flatMap { map =>
                                      val retryState = map(webhookId)
                                      retryState.backoff.map(retryQueue.take.delay(_)).getOrElse(retryQueue.take)
                                    }
                           event <- shutdownSignal.await.disconnect.raceEither(take.disconnect).map(_.toOption)
                           _     <- ZIO.foreachDiscard(event) { event =>
                                      val webhookId = event.key.webhookId
                                      for {
                                        _           <- activateWithTimeout
                                        webhook     <- webhooksProxy.getWebhookById(webhookId)
                                        deliverEvent = (batchDispatcher, webhook.batching) match {
                                                         case (Some(batchDispatcher), WebhookDeliveryBatching.Batched) =>
                                                           batchDispatcher.enqueueEvent(event)
                                                         case _                                                        =>
                                                           val dispatch = WebhookDispatch(
                                                             webhook.id,
                                                             webhook.url,
                                                             webhook.deliveryMode.semantics,
                                                             WebhookPayload.Single(event)
                                                           )
                                                           retryEvents(dispatch, None)
                                                       }
                                        _           <- if (webhook.isEnabled)
                                                         deliverEvent
                                                       else
                                                         retryStates.updateZIO { retries =>
                                                           retries(webhook.id).deactivate
                                                             .map(retries.updated(webhook.id, _))
                                                         }
                                      } yield ()
                                    }
                         } yield ()
      isShutdown      <- shutdownSignal.isDone
      _               <- handleEvent
                           .repeatUntilZIO(_ => shutdownSignal.isDone)
                           .onError(fatalPromise.fail)
                           .fork
                           .unless(isShutdown)
                           .provideLayer(ZLayer.succeed(clock))
    } yield ()
  }
}
