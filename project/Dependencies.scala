import sbt.*

object Dependencies {

  object Akka {
    private val version = "2.6.21"
    val Stream = "com.typesafe.akka" %% "akka-stream" % version
    val Actor = "com.typesafe.akka" %% "akka-actor" % version
    val AkkaPersistence = "com.typesafe.akka" %% "akka-persistence" % version
    val Cluster = "com.typesafe.akka" %% "akka-cluster" % version
    val ClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version
    val TestKit = "com.typesafe.akka" %% "akka-testkit" % version
  }

  val ScalaTest = "org.scalatest" %% "scalatest" % "3.2.20"
  val Logback = "ch.qos.logback" % "logback-classic" % "1.6.1"
  val Logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6"
  val Nel = "com.evolutiongaming" %% "nel" % "1.3.5"
  val ScalaTools = "com.evolutiongaming" %% "scala-tools" % "3.0.6"
  val ConfigTools = "com.evolutiongaming" %% "config-tools" % "1.0.5"

  object Prometheus {
    // fixing the prometheus version in place because we use a 0.9.999-evo1 fork internally
    private val version = "0.9.0" // scala-steward:off
    val simpleclient = "io.prometheus" % "simpleclient" % version
  }
}
