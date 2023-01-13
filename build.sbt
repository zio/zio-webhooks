import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev/zio-webhooks/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      ),
      Developer(
        "softinio",
        "Salar Rahmanian",
        "code@softinio.com",
        url("https://www.softinio.com")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-webhooks/"), "scm:git:git@github.com:zio/zio-webhooks.git")
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion        = "2.0.4"
val zioHttpVersion    = "2.0.0-RC11"
val zioJson           = "0.4.1"
val zioPreludeVersion = "1.0.0-RC16"
val sttpVersion       = "3.8.3"

lazy val `zio-webhooks` =
  project
    .in(file("."))
    .settings(publish / skip := true)
    .aggregate(zioWebhooksCore, zioWebhooksTest, webhooksTestkit, examples)

lazy val zioWebhooksCore = module("zio-webhooks-core", "webhooks")
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoSettings("zio.webhooks"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"                       %% "zio"         % zioVersion,
      "dev.zio"                       %% "zio-json"    % zioJson,
      "dev.zio"                       %% "zio-prelude" % zioPreludeVersion,
      "dev.zio"                       %% "zio-streams" % zioVersion,
      "dev.zio"                       %% "zio-test"    % zioVersion,
      "com.softwaremill.sttp.client3" %% "core"        % sttpVersion,
      "com.softwaremill.sttp.client3" %% "zio"         % sttpVersion
    )
  )
  .settings(
    stdSettings("zio-webhooks")
  )

lazy val zioWebhooksTest = module("zio-webhooks-test", "webhooks-test")
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    publish / skip := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion     % "it,test",
      "dev.zio" %% "zio-test-sbt" % zioVersion     % "it,test",
      "dev.zio" %% "zio-json"     % zioJson        % "it",
      "io.d11"  %% "zhttp"        % zioHttpVersion % "it"
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(zioWebhooksCore, webhooksTestkit)

lazy val webhooksTestkit = module("zio-webhooks-testkit", "webhooks-testkit")
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % "test",
      "dev.zio" %% "zio-test-sbt" % zioVersion % "test"
    )
  )
  .dependsOn(zioWebhooksCore)

lazy val examples = module("zio-webhooks-examples", "examples")
  .settings(
    publish / skip := true,
    fork := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % "test",
      "dev.zio" %% "zio-test-sbt" % zioVersion % "test",
      "io.d11"  %% "zhttp"        % zioHttpVersion
    ),
    testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(zioWebhooksCore, webhooksTestkit)

def module(moduleName: String, fileName: String): Project =
  Project(moduleName, file(fileName))
    .settings(stdSettings(moduleName))
    .settings(
      libraryDependencies ++= Seq(
        "dev.zio" %% "zio" % zioVersion
      )
    )

lazy val docs = project
  .in(file("zio-webhooks-docs"))
  .settings(
    moduleName := "zio-webhooks-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq("dev.zio" %% "zio" % zioVersion),
    projectName := "ZIO Webhook",
    mainModuleName := (zioWebhooksCore / moduleName).value,
    projectStage := ProjectStage.Development,
    docsPublishBranch := "series/2.x"
  )
  .dependsOn(zioWebhooksCore)
  .enablePlugins(WebsitePlugin)
