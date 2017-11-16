import Dependencies._
import sbt.Keys.{homepage, organizationName, startYear}

lazy val thisBuildSettings = inThisBuild(List(
  scalaVersion := "2.12.4"
))

lazy val commonSettings = Seq(
  scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture"
  ),
  crossScalaVersions := Seq("2.12.4", "2.11.11"),
  resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")
)

lazy val publishSettings = Seq(
  homepage := Some(new URL("http://github.com/evolution-gaming/akka-tools")),
  startYear := Some(2016),
  organizationName := "Evolution Gaming",
  organizationHomepage := Some(url("http://evolutiongaming.com")),
  bintrayOrganization := Some("evolutiongaming"),
  releaseCrossBuild := true,
  organization := "com.evolutiongaming",
  licenses := Seq("MIT" -> url("http://www.opensource.org/licenses/mit-license.html"))
)

lazy val allSettings = thisBuildSettings ++ commonSettings ++ publishSettings

lazy val akkaTools = (project
  in file(".")
  settings (name := "akka-tools")
  settings allSettings
  aggregate(instrumentation, cluster, persistence, serialization, util, test, replicate2kafka))

lazy val instrumentation = (project
  in file("instrumentation")
  dependsOn util
  settings (name := "akka-tools-instrumentation")
  settings (libraryDependencies ++= Seq(Akka.Actor, ScalaTools, MetricTools, MetricsCore))
  settings allSettings)

lazy val cluster = (project
  in file("cluster")
  dependsOn (test % "test->compile")
  settings (name := "akka-tools-cluster")
  settings (libraryDependencies ++= Seq(Akka.Actor, Akka.Cluster, Akka.ClusterSharding, Akka.TestKit % Test,
    Logging, MetricsCore, ScalaTools, MockitoCore % Test, ScalaTest % Test))
  settings allSettings)

lazy val persistence = (project
  in file("persistence")
  dependsOn (serialization, test % "test->compile")
  settings (name := "akka-tools-persistence")
  settings (libraryDependencies ++= Seq(Akka.Actor, Akka.AkkaPersistence, ScalaTools,
    Akka.TestKit % "test", ScalaTest % "test"))
  settings allSettings)

lazy val serialization = (project
  in file("serialization")
  dependsOn (test % "test->compile")
  settings (name := "akka-tools-serialization")
  settings (libraryDependencies ++= Seq(Akka.Actor, Logging, ChillAkka, PlayJson, scalax,
    Akka.AkkaPersistence, playJsonTools, ScalaTest % "test"))
  settings allSettings)

lazy val util = (project
  in file("util")
  dependsOn (test % "test->compile")
  settings (name := "akka-tools-util")
  settings (libraryDependencies ++= Seq(Akka.Actor, Akka.TestKit % "test", ScalaTest % "test",
    MetricTools, MetricsCore, Logging, Guava))
  settings allSettings)

lazy val test = (project
  in file("test")
  settings (name := "akka-tools-test")
  settings (libraryDependencies ++= Seq(Akka.Actor, Akka.TestKit, ScalaTest, Guava))
  settings allSettings)

lazy val replicate2kafka = (project
  in file("replicate2kafka")
  dependsOn (persistence, test)
  settings (name := "akka-tools-replicate2kafka")
  settings (libraryDependencies ++=
    Seq(Akka.Actor, Akka.AkkaPersistence, Logging, kafkaClients, kafkaStream, ScalaTools, ScalaTest % Test, MockitoCore % Test))
  settings allSettings)