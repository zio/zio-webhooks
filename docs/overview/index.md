---
id: overview_index
title: "Contents"
---

ZIO Webhooks - a microlibrary for reliable and persistent webhook delivery.
 
## Installation

Include ZIO Webhooks in your project by adding the following to your `build.sbt`:

```scala mdoc:passthrough

println(s"""```""")
if (zio.webhooks.BuildInfo.isSnapshot)
  println(s"""resolvers += Resolver.sonatypeRepo("snapshots")""")
println(s"""libraryDependencies += "dev.zio" %% "zio-webhooks" % "${zio.webhooks.BuildInfo.version}"""")
println(s"""```""")

```
