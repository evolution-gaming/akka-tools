import Dependencies._
import sbt.Keys.{homepage, organizationName, startYear}

lazy val commonSettings = Seq(
  scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
  scalaVersion := crossScalaVersions.value.head,
  crossScalaVersions := Seq("2.12.8"),
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

lazy val allSettings = commonSettings ++ publishSettings

lazy val akkaTools = (project
  in file(".")
  settings (name := "akka-tools")
  settings allSettings                        
  aggregate(instrumentation, cluster, persistence, serialization, util, test))

lazy val instrumentation = (project
  in file("instrumentation")
  dependsOn util
  settings (
    name := "akka-tools-instrumentation",
    libraryDependencies ++= Seq(
      Akka.Actor,
      ConfigTools,
      Prometheus.simpleclient))
  settings allSettings)

lazy val cluster = (project
  in file("cluster")
  dependsOn (test % "test->compile")
  settings(
    name := "akka-tools-cluster",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Akka.Cluster,
      Akka.ClusterSharding,
      Akka.TestKit % Test,
      Logging,
      ConfigTools,
      Nel,
      scalax,
      ScalaTest % Test))
  settings allSettings)

lazy val persistence = (project
  in file("persistence")
  dependsOn (serialization, test % "test->compile")
  settings(
    name := "akka-tools-persistence",
    libraryDependencies ++= Seq(
      Akka.Actor,
      ConfigTools,
      Akka.TestKit % Test,
      ScalaTest % Test))
  settings allSettings)

lazy val serialization = (project
  in file("serialization")
  dependsOn (test % "test->compile")
  settings(
    name := "akka-tools-serialization",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Logging,
      scalax,
      Akka.AkkaPersistence,
      ScalaTest % Test))
  settings allSettings)

lazy val util = (project
  in file("util")
  dependsOn (test % "test->compile")
  settings(
    name := "akka-tools-util",
    libraryDependencies ++= Seq(
      Akka.Actor,
      Akka.TestKit % Test,
      ScalaTest % Test,
      Logging))
  settings allSettings)

lazy val test = (project
  in file("test")
  settings(
    name := "akka-tools-test",
    libraryDependencies ++= Seq(
        Akka.Actor, 
        Akka.TestKit, 
        ScalaTest))
  settings allSettings)