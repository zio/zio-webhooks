package zio.webhooks

import zio.test.DefaultRunnableSpec
import zio.test._
import zio.webhooks.testkit.TestWebhookRepo
import zio.webhooks.testkit.TestWebhookStateRepo
import zio.webhooks.testkit.TestWebhookEventRepo

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      test("attempts to send as many events as repo publishes") {
        // TODO: write ways to manipulate contents TestWebhook*Repo

        assertCompletes
      }
    )

  // TODO: build server layer with testkit to use in test
  def buildServer = {
    (TestWebhookRepo.test ++ TestWebhookStateRepo.test) >>> TestWebhookEventRepo.test
    ???
  }
}
