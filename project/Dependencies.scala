import sbt._

object Dependencies {

  object Akka {
    private val akkaVersion = "2.5.6"

    val Stream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
    val Actor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
    val Typed = "com.typesafe.akka" %% "akka-typed" % akkaVersion
    val Slf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
    val AkkaPersistence = "com.typesafe.akka" %% "akka-persistence" % akkaVersion

    val StreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion

    val Cluster = "com.typesafe.akka" %% "akka-cluster" % akkaVersion % Compile
    val ClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion % Compile

    val TestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
  }

  private val scalaTestVersion = "3.0.4"
  private val logbackVersion = "1.2.3"
  private val scalaLoggingVersion = "3.7.2"

  lazy val ScalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
  lazy val MetricTools = "com.evolutiongaming" %% "metric-tools" % "0.4"
  lazy val MetricsCore = "io.dropwizard.metrics" % "metrics-core" % "3.2.2"
  lazy val Guava = "com.google.guava" % "guava" % "19.0"
  lazy val Logback = "ch.qos.logback" % "logback-classic" % logbackVersion
  lazy val Logging = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
  lazy val ScalaTools = "com.evolutiongaming" %% "scala-tools" % "1.11"
  lazy val MockitoCore = "org.mockito" % "mockito-core" % "1.9.5"
  lazy val ChillAkka = "com.twitter" %% "chill-akka" % "0.9.2" % Compile excludeAll(
    ExclusionRule("org.ow2.asm", "asm"))
  lazy val playJsonTools = "com.evolutiongaming" %% "play-json-tools" % "0.1.0"

  val scalax = "com.github.t3hnar" %% "scalax" % "3.2" % Compile

  lazy val PlayJson = "com.typesafe.play" %% "play-json" % "2.6.6"
}