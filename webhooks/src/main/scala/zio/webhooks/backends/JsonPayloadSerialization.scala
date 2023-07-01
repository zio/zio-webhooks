package zio.webhooks.backends

import zio.{ Chunk, Has, ULayer, ZLayer }
import zio.webhooks._

object JsonPayloadSerialization {

  val live: ULayer[Has[SerializePayload]] =
    ZLayer.succeed { (webhookPayload: WebhookPayload, contentType: Option[WebhookContentMimeType]) =>
      contentType match {
        case Some(WebhookContentMimeType(contentType)) if contentType.toLowerCase == "application/json" =>
          webhookPayload match {
            case WebhookPayload.Single(event)   =>
              (event.content, Chunk.empty)
            case WebhookPayload.Batched(events) =>
              ("[" + events.map(_.content).mkString(",") + "]", Chunk.empty)
          }
        case _                                                                                          =>
          webhookPayload match {
            case WebhookPayload.Single(event)   =>
              (event.content, Chunk.empty)
            case WebhookPayload.Batched(events) =>
              (events.map(_.content).mkString, Chunk.empty)
          }
      }
    }
}
