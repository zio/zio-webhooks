[//]: # (This file was autogenerated using `zio-sbt-website` plugin via `sbt generateReadme` command.)
[//]: # (So please do not edit it manually. Instead, change "docs/index.md" file or sbt setting keys)
[//]: # (e.g. "readmeDocumentation" and "readmeSupport".)

# ZIO Webhooks

ZIO Webhooks is a microlibrary for reliable and persistent webhook delivery.

[![Development](https://img.shields.io/badge/Project%20Stage-Development-green.svg)](https://github.com/zio/zio/wiki/Project-Stages) ![CI Badge](https://github.com/zio/zio-webhooks/workflows/CI/badge.svg) [![Sonatype Releases](https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-webhooks_2.13.svg?label=Sonatype%20Release)](https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-webhooks_2.13/) [![Sonatype Snapshots](https://img.shields.io/nexus/s/https/oss.sonatype.org/dev.zio/zio-webhooks_2.13.svg?label=Sonatype%20Snapshot)](https://oss.sonatype.org/content/repositories/snapshots/dev/zio/zio-webhooks_2.13/) [![javadoc](https://javadoc.io/badge2/dev.zio/zio-webhooks-docs_2.13/javadoc.svg)](https://javadoc.io/doc/dev.zio/zio-webhooks-docs_2.13) [![ZIO Webhooks](https://img.shields.io/github/stars/zio/zio-webhooks?style=social)](https://github.com/zio/zio-webhooks)

## Introduction

Below is a state diagram for each webhook handled by a server. Note that there aren't any initial or final states as the server doesn't manage the entire lifecycle of webhooks and events—only the subset needed for reliable webhook delivery.

## Installation

Include ZIO Webhooks in your project by adding the following to your `build.sbt`:

```scala
libraryDependencies += "dev.zio" %% "zio-webhooks" % "0.2.1"
```

# Getting Started

There are two ways to start a webhook server: as part of the managed construction of its live layer
`WebhookServer.live`, or manually by calling `WebhookServer.start`. See [examples](#example-programs) for a list of code
examples. The managed approach is recommended as it guarantees the server shuts down gracefully. Make sure to
call `shutdown` on a server created manually. Either way, users will have to implement the following dependencies that are part of `WebhookServer`'s environment:

* [`WebhookRepo`](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/webhooks/src/main/scala/zio/webhooks/WebhookRepo.scala) - to get webhooks and update webhook status;
* [`WebhookEventRepo`](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/webhooks/src/main/scala/zio/webhooks/WebhookEventRepo.scala) - to subscribe to events and
  update event status;
* [`WebhookHttpClient`](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/webhooks/src/main/scala/zio/webhooks/WebhookHttpClient.scala) - to deliver events via HTTP POST
  (docs/[WebhookSttpClient](https://github.com/zio/zio-webhooks/tree/series/2.x/webhooks/src/main/scala/zio/webhooks/backends/sttp/WebhookSttpClient.scala) is provided and ready to use);
* [`WebhookServerConfig`](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/webhooks/src/main/scala/zio/webhooks/WebhookServerConfig.scala) - to specify settings for
  error hub capacity, retrying, and batching; and
* `SerializePayload` - for specifying custom serialization of single and batched event payloads of some content type.

[![](https://mermaid.ink/img/eyJjb2RlIjoic3RhdGVEaWFncmFtLXYyXG4gICAgRW5hYmxlZCAtLT4gRGlzYWJsZWQgOiBkaXNhYmxlZCBleHRlcm5hbGx5XG4gICAgRW5hYmxlZCAtLT4gRW5hYmxlZCA6IGV2ZW50IGRpc3BhdGNoIHN1Y2Nlc3NcbiAgICBEaXNhYmxlZCAtLT4gRW5hYmxlZCA6IGVuYWJsZWQgZXh0ZXJuYWxseVxuICAgIEVuYWJsZWQgLS0-IFJldHJ5aW5nIDogZXZlbnQgZGlzcGF0Y2ggZmFpbHVyZVxuICAgIFJldHJ5aW5nIC0tPiBFbmFibGVkIDogcmV0cnkgcXVldWUgZW1wdGllZFxuICAgIFJldHJ5aW5nIC0tPiBVbmF2YWlsYWJsZSA6IGNvbnRpbnVvdXMgZmFpbHVyZSBmb3IgZHVyYXRpb24gRFxuICAgIFVuYXZhaWxhYmxlIC0tPiBFbmFibGVkIDogZW5hYmxlZCBleHRlcm5hbGx5XG4gICAgVW5hdmFpbGFibGUgLS0-IERpc2FibGVkIDogZGlzYWJsZWQgZXh0ZXJuYWxseSIsIm1lcm1haWQiOnt9LCJ1cGRhdGVFZGl0b3IiOmZhbHNlfQ)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoic3RhdGVEaWFncmFtLXYyXG4gICAgRW5hYmxlZCAtLT4gRGlzYWJsZWQgOiBkaXNhYmxlZCBleHRlcm5hbGx5XG4gICAgRW5hYmxlZCAtLT4gRW5hYmxlZCA6IGV2ZW50IGRpc3BhdGNoIHN1Y2Nlc3NcbiAgICBEaXNhYmxlZCAtLT4gRW5hYmxlZCA6IGVuYWJsZWQgZXh0ZXJuYWxseVxuICAgIEVuYWJsZWQgLS0-IFJldHJ5aW5nIDogZXZlbnQgZGlzcGF0Y2ggZmFpbHVyZVxuICAgIFJldHJ5aW5nIC0tPiBFbmFibGVkIDogcmV0cnkgcXVldWUgZW1wdGllZFxuICAgIFJldHJ5aW5nIC0tPiBVbmF2YWlsYWJsZSA6IGNvbnRpbnVvdXMgZmFpbHVyZSBmb3IgZHVyYXRpb24gRFxuICAgIFVuYXZhaWxhYmxlIC0tPiBFbmFibGVkIDogZW5hYmxlZCBleHRlcm5hbGx5XG4gICAgVW5hdmFpbGFibGUgLS0-IERpc2FibGVkIDogZGlzYWJsZWQgZXh0ZXJuYWxseSIsIm1lcm1haWQiOnt9LCJ1cGRhdGVFZGl0b3IiOmZhbHNlfQ)
```mermaid
stateDiagram-v2
    Enabled --> Disabled : disabled externally
    Enabled --> Enabled : event dispatch success
    Disabled --> Enabled : enabled externally
    Enabled --> Retrying : event dispatch failure
    Retrying --> Enabled : retry queue emptied
    Retrying --> Unavailable : continuous failure for duration D
    Unavailable --> Enabled : enabled externally
    Unavailable --> Disabled : disabled externally
```
 
# Example Programs

* [Basic example](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/BasicExample.scala) - most basic usage with the default configuration.
* [Basic example with batching](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/BasicExampleWithBatching.scala) - same as the basic example but with batching enabled.
* [Basic example with retrying](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/BasicExampleWithRetrying.scala) - shows how the server retries when an endpoint fails sometimes.
* [Custom config example](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/CustomConfigExample.scala) - a custom configuration example that also showcases batched retries.
* [Event recovery example](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/EventRecoveryExample.scala) - shows how the server continues retries after being restarted.
* [Manual server example](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/ManualServerExample.scala) - shows how to create, start, and shut down a server programmatically.
* [Shutdown on first error](docs/https://github.com/zio/zio-webhooks/tree/series/2.x/examples/src/main/scala/zio/webhooks/example/ShutdownOnFirstError.scala) - shows how to shut down the server on the first error encountered.

## Documentation

Learn more on the [ZIO Webhooks homepage](https://zio.dev/zio-webhooks/)!

## Contributing

For the general guidelines, see ZIO [contributor's guide](https://zio.dev/about/contributing).

## Code of Conduct

See the [Code of Conduct](https://zio.dev/about/code-of-conduct)

## Support

Come chat with us on [![Badge-Discord]][Link-Discord].

[Badge-Discord]: https://img.shields.io/discord/629491597070827530?logo=discord "chat on discord"
[Link-Discord]: https://discord.gg/2ccFBr4 "Discord"

## License

[License](LICENSE)
