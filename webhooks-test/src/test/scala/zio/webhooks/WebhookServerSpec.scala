package zio.webhooks

// import zio.webhooks.testkit._
import zio.test.DefaultRunnableSpec
import zio.test._

object WebhookServerSpec extends DefaultRunnableSpec {
  def spec =
    suite("WebhookServerSpec")(
      test("stub")(assertCompletes)
    )
}
