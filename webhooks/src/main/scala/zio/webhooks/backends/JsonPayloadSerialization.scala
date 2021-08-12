package zio.webhooks.backends

import zio.{ Has, ULayer, ZLayer }
import zio.webhooks.{ SerializePayload, WebhookPayload }

object JsonPayloadSerialization {

  val live: ULayer[Has[SerializePayload]] =
    ZLayer.succeed { (webhookPayload: WebhookPayload, contentType: Option[String]) =>
      contentType match {
        case Some("application/json") =>
          webhookPayload match {
            case WebhookPayload.Single(event)   =>
              event.content
            case WebhookPayload.Batched(events) =>
              "[" + events.map(_.content).mkString(",") + "]"
          }
        case _                        =>
          webhookPayload match {
            case WebhookPayload.Single(event)   =>
              event.content
            case WebhookPayload.Batched(events) =>
              events.map(_.content).mkString
          }
      }
    }
}
