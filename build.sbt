name := "akka-tools"

organization := "com.evolutiongaming"

homepage := Some(new URL("http://github.com/evolution-gaming/akka-tools"))

startYear := Some(2016)

organizationName := "Evolution Gaming"

organizationHomepage := Some(url("http://evolutiongaming.com"))

bintrayOrganization := Some("evolutiongaming")

scalaVersion := "2.11.8"

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
)

val AkkaVersion = "2.4.11"
val MetricsVersion = "3.1.2"
val MockitoVersion = "1.9.5"

resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies ++= Seq(
  "com.github.t3hnar" %% "scalax" % "3.0",
  "com.evolutiongaming" %% "scala-tools" % "0.1",
  "io.dropwizard.metrics" % "metrics-core" % MetricsVersion,
  "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding" % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % AkkaVersion,
  "com.typesafe.akka" %% "akka-distributed-data-experimental" % AkkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.0.0" % Test,
  "org.mockito" % "mockito-core" % MockitoVersion % Test

)

licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
