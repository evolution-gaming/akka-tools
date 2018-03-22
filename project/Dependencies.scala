import sbt._

object Dependencies {

  object Akka {
    private val version = "2.5.11"

    val Stream =          "com.typesafe.akka" %% "akka-stream" % version
    val Actor =           "com.typesafe.akka" %% "akka-actor" % version
    val Typed =           "com.typesafe.akka" %% "akka-typed" % version
    val Slf4j =           "com.typesafe.akka" %% "akka-slf4j" % version
    val AkkaPersistence = "com.typesafe.akka" %% "akka-persistence" % version
    val StreamTestKit =   "com.typesafe.akka" %% "akka-stream-testkit" % version
    val Cluster =         "com.typesafe.akka" %% "akka-cluster" % version % Compile
    val ClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % version % Compile
    val TestKit =         "com.typesafe.akka" %% "akka-testkit" % version
  }

  lazy val ScalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val MetricTools = "com.evolutiongaming" %% "metric-tools" % "1.1"
  lazy val MetricsCore = "io.dropwizard.metrics" % "metrics-core" % "3.2.6"
  lazy val Guava = "com.google.guava" % "guava" % "23.0"
  lazy val Logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val Logging = "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"
  lazy val ScalaTools = "com.evolutiongaming" %% "scala-tools" % "2.0"
  lazy val MockitoCore = "org.mockito" % "mockito-core" % "2.12.0"
  lazy val ChillAkka = "com.twitter" %% "chill-akka" % "0.9.2" % Compile excludeAll ExclusionRule("org.ow2.asm", "asm")
  lazy val playJsonTools = "com.evolutiongaming" %% "play-json-tools" % "0.2.0"
  lazy val scalax = "com.github.t3hnar" %% "scalax" % "3.3" % Compile
  lazy val Nel = "com.evolutiongaming" %% "nel" % "1.1"
  lazy val ConfigTools = "com.evolutiongaming" %% "config-tools" % "1.0.1"
}