import Dependencies.*
import com.typesafe.tools.mima.core.*
import sbt.Keys.*
import sbtversionpolicy.Compatibility.BinaryCompatible

lazy val commonSettings = Seq(
  Compile / doc / scalacOptions ++= Seq("-no-link-warnings"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.13.18", "3.9.0"),
  publishTo := Some(Resolver.evolutionReleases),
  versionScheme := Some("semver-spec"),
  versionPolicyIntention := BinaryCompatible,
  versionPolicyIgnored ++= Seq(
    // add libraries here that are known to be binary compatible, like:
    // TODO comment after next release
    "io.prometheus" % "simpleclient",
  ),
  mimaBinaryIssueFilters ++= Seq(
    // add mima check exceptions here, like:
    // TODO comment after next release
    ProblemFilters.exclude[DirectMissingMethodProblem](
      "com.evolutiongaming.util.dispatchers.Instrumented#OverrideRunnable#2#Default.this",
    ),
  ),
)

lazy val publishSettings = Seq(
  homepage := Some(uri("http://github.com/evolution-gaming/akka-tools")),
  startYear := Some(2016),
  organizationName := "Evolution",
  organizationHomepage := Some(uri("http://evolution.com")),
  organization := "com.evolutiongaming",
  licenses := Seq("MIT" -> uri("http://www.opensource.org/licenses/mit-license.html")),
)

lazy val allSettings = commonSettings ++ publishSettings

lazy val akkaTools = project
  .in(file("."))
  .settings(name := "akka-tools")
  .settings(allSettings)
  .aggregate(instrumentation, cluster, persistence, serialization, util, test)

lazy val instrumentation = project
  .in(file("instrumentation"))
  .dependsOn(util)
  .settings(
    name := "akka-tools-instrumentation",
    libraryDependencies ++= Seq(
      Akka.Actor,
      ConfigTools,
      Prometheus.simpleclient,
    ),
  )
  .settings(allSettings)

lazy val cluster = project
  .in(file("cluster"))
  .dependsOn(test % "test->compile")
  .settings(
    name := "akka-tools-cluster",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Akka.Cluster,
      Akka.ClusterSharding,
      Akka.TestKit % Test,
      Logging,
      ConfigTools,
      Nel,
      ScalaTest % Test,
    ),
  )
  .settings(allSettings)

lazy val persistence = project
  .in(file("persistence"))
  .dependsOn(serialization, test % "test->compile")
  .settings(
    name := "akka-tools-persistence",
    libraryDependencies ++= Seq(
      Akka.Actor,
      ScalaTools,
      ConfigTools,
      Akka.TestKit % Test,
      ScalaTest % Test,
    ),
  )
  .settings(allSettings)

lazy val serialization = project
  .in(file("serialization"))
  .dependsOn(test % "test->compile")
  .settings(
    name := "akka-tools-serialization",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Logging,
      Akka.AkkaPersistence,
      ScalaTest % Test,
    ),
  )
  .settings(allSettings)

lazy val util = project
  .in(file("util"))
  .dependsOn(test % "test->compile")
  .settings(
    name := "akka-tools-util",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Akka.TestKit % Test,
      ScalaTest % Test,
      Logging,
    ),
  )
  .settings(allSettings)

lazy val test = project
  .in(file("test"))
  .settings(
    name := "akka-tools-test",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Akka.TestKit,
      ScalaTest,
    ),
  )
  .settings(allSettings)

// check is called with + from the release action
addCommandAlias("check", "all versionPolicyCheck Compile/doc scalafmtCheckRepo")
addCommandAlias("fmt", "+all scalafmtRepo")
addCommandAlias("build", "all compile testFull")
