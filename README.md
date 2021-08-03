# zio-webhooks

| Project Stage | CI | Release | Snapshot | Discord |
| --- | --- | --- | --- | --- |
| [![Project stage][Stage]][Stage-Page] | ![CI][Badge-CI] | [![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases] | [![Snapshot Artifacts][Badge-SonatypeSnapshots]][Link-SonatypeSnapshots] | [![Badge-Discord]][Link-Discord] |

# Summary

Microlibrary for reliable and persistent webhook delivery.

# Getting Started

There are two ways to start a webhook server: as part of the managed construction of its live layer
`WebhookServer.live`, or manually by calling `WebhookServer.start`. See [examples](#example-programs) for a list of code
examples. The managed approach is recommended as it guarantees the server shuts down gracefully. Make sure to
call `shutdown` on a server created manually. Either way, the server requires the following dependencies in its environment:

* [`WebhookRepo`](/webhooks/src/main/scala/zio/webhooks/WebhookRepo.scala) - to get webhooks and update webhook status;
* [`WebhookEventRepo`](/webhooks/src/main/scala/zio/webhooks/WebhookEventRepo.scala) - to subscribe to events and
  update event status;
* [`WebhookHttpClient`](/webhooks/src/main/scala/zio/webhooks/WebhookHttpClient.scala) - to deliver events via HTTP POST
  ([WebhookSttpClient](/webhooks/src/main/scala/zio/webhooks/backends/sttp/WebhookSttpClient.scala) is provided); and
* [`WebhookServerConfig`](/webhooks/src/main/scala/zio/webhooks/WebhookServerConfig.scala) - to specify settings for
  error hub capacity, retrying, and batching.

# Example Programs

* [Basic example](/examples/src/main/scala/zio/webhooks/example/BasicExample.scala) - most basic usage with the default configuration. 
* [Basic example with batching](/examples/src/main/scala/zio/webhooks/example/BasicExampleWithBatching.scala) - same as the basic example but with batching enabled.
* [Basic example with retrying](/examples/src/main/scala/zio/webhooks/example/BasicExampleWithRetrying.scala) - shows how the server retries when an endpoint fails sometimes.
* [Custom config example](/examples/src/main/scala/zio/webhooks/example/CustomConfigExample.scala) - a custom configuration example that also showcases batched retries.
* [Event recovery example](/examples/src/main/scala/zio/webhooks/example/EventRecoveryExample.scala) - shows how the server continues retries after being restarted.
* [Manual server example](/examples/src/main/scala/zio/webhooks/example/ManualServerExample.scala) - shows how to create, start, and shut down a server programmatically.
* [Shutdown on first error](/examples/src/main/scala/zio/webhooks/example/ShutdownOnFirstError.scala) - shows how to shut down the server on the first error encountered.

# Documentation

[ZIO Webhooks Microsite](https://zio.github.io/zio-webhooks/)

# Contributing

[Documentation for contributors](https://zio.github.io/zio-webhooks/docs/about/about_contributing)

## Code of Conduct

See the [Code of Conduct](https://zio.github.io/zio-webhooks/docs/about/about_coc)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].

# License

[License](LICENSE)

[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-webhooks_2.12.svg "Sonatype Releases"
[Badge-SonatypeSnapshots]: https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-webhooks_2.12.svg "Sonatype Snapshots"
[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord "chat on discord"
[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-webhooks_2.12/ "Sonatype Releases"
[Link-SonatypeSnapshots]: https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-webhooks_2.12/ "Sonatype Snapshots"
[Link-Discord]: https://discord.gg/2ccFBr4 "Discord"
[Badge-CI]: https://github.com/zio/zio-webhooks/workflows/CI/badge.svg
[Stage]: https://img.shields.io/badge/Project%20Stage-Concept-red.svg
[Stage-Page]: https://github.com/zio/zio/wiki/Project-Stages

