import sbt._

object Dependencies {

  object Akka {
    private val version = "2.5.18"
    val Stream          = "com.typesafe.akka" %% "akka-stream" % version
    val Actor           = "com.typesafe.akka" %% "akka-actor" % version
    val AkkaPersistence = "com.typesafe.akka" %% "akka-persistence" % version
    val Cluster         = "com.typesafe.akka" %% "akka-cluster" % version % Compile
    val ClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version % Compile
    val TestKit         = "com.typesafe.akka" %% "akka-testkit" % version
  }

  val ScalaTest     = "org.scalatest" %% "scalatest" % "3.0.5"
  val MetricsCore   = "io.dropwizard.metrics" % "metrics-core" % "3.2.6"
  val Logback       = "ch.qos.logback" % "logback-classic" % "1.2.3"
  val Logging       = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  val ChillAkka     = "com.twitter" %% "chill-akka" % "0.9.3" excludeAll ExclusionRule("org.ow2.asm", "asm")
  val scalax        = "com.github.t3hnar" %% "scalax" % "3.4"
  val playJsonTools = "com.evolutiongaming" %% "play-json-tools" % "0.3.8"
  val Nel           = "com.evolutiongaming" %% "nel" % "1.3.3"
  val MetricTools   = "com.evolutiongaming" %% "metric-tools" % "1.1"
  val ScalaTools    = "com.evolutiongaming" %% "scala-tools" % "2.0"
  val ConfigTools   = "com.evolutiongaming" %% "config-tools" % "1.0.3"
}