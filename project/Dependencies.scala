import sbt._

object Dependencies {

  object Akka {
    private val version = "2.6.8"
    val Stream          = "com.typesafe.akka" %% "akka-stream"           % version
    val Actor           = "com.typesafe.akka" %% "akka-actor"            % version
    val AkkaPersistence = "com.typesafe.akka" %% "akka-persistence"      % version
    val Cluster         = "com.typesafe.akka" %% "akka-cluster"          % version
    val ClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version
    val TestKit         = "com.typesafe.akka" %% "akka-testkit"          % version
  }

  val ScalaTest     = "org.scalatest"              %% "scalatest"       % "3.0.9"
  val Logback       = "ch.qos.logback"              % "logback-classic" % "1.2.3"
  val scalax        = "com.github.t3hnar"          %% "scalax"          % "3.8.1"
  val Logging       = "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.2"
  val Nel           = "com.evolutiongaming"        %% "nel"             % "1.3.4"
  val ScalaTools    = "com.evolutiongaming"        %% "scala-tools"     % "3.0.5"
  val ConfigTools   = "com.evolutiongaming"        %% "config-tools"    % "1.0.5"

  object Prometheus {
    private val version = "0.8.1"
    val simpleclient = "io.prometheus" % "simpleclient" % version
  }
}