import sbt._

object Dependencies {

  object Akka {
    private val version = "2.6.21"
    val Stream          = "com.typesafe.akka" %% "akka-stream"           % version
    val Actor           = "com.typesafe.akka" %% "akka-actor"            % version
    val AkkaPersistence = "com.typesafe.akka" %% "akka-persistence"      % version
    val Cluster         = "com.typesafe.akka" %% "akka-cluster"          % version
    val ClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version
    val TestKit         = "com.typesafe.akka" %% "akka-testkit"          % version
  }

  val ScalaTest     = "org.scalatest"              %% "scalatest"       % "3.2.18"
  val Logback       = "ch.qos.logback"              % "logback-classic" % "1.2.3"
  val Logging       = "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.5"
  val Nel           = "com.evolutiongaming"        %% "nel"             % "1.3.5"
  val ScalaTools    = "com.evolutiongaming"        %% "scala-tools"     % "3.0.6"
  val ConfigTools   = "com.evolutiongaming"        %% "config-tools"    % "1.0.5"

  object Prometheus {
    private val version = "0.16.0"
    val simpleclient = "io.prometheus" % "simpleclient" % version
  }
}