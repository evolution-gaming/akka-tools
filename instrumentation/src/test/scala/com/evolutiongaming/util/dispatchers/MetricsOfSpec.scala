package com.evolutiongaming.util.dispatchers

import io.prometheus.client.CollectorRegistry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsOfSpec extends AnyWordSpec with Matchers {

  private val Dispatcher = "akka-test-dispatcher"

  "Instrumented.Metrics.Of" should {

    "not expose dispatcher series before the dispatcher has run anything" in {
      val registry = new CollectorRegistry()
      Instrumented.Metrics.Of(registry).apply(Dispatcher)

      latencyCount(registry, "queue") shouldBe None
      workers(registry) shouldBe None
    }

    "observe queue and run latency in seconds and track busy workers" in {
      val registry = new CollectorRegistry()
      val metrics = Instrumented.Metrics.Of(registry).apply(Dispatcher)

      metrics.queue(latencyNanos = 2000000L)
      workers(registry) shouldBe Some(1.0)

      metrics.run(latencyNanos = 3000000L)
      workers(registry) shouldBe Some(0.0)

      latencyCount(registry, "queue") shouldBe Some(1.0)
      latencySum(registry, "queue") shouldBe Some(0.002)
      latencyCount(registry, "run") shouldBe Some(1.0)
      latencySum(registry, "run") shouldBe Some(0.003)
    }

    "use the given prefix" in {
      val registry = new CollectorRegistry()
      Instrumented.Metrics.Of(registry, prefix = "custom").apply(Dispatcher).queue(latencyNanos = 1000000L)

      sampleValue(registry, "custom_workers", "dispatcher" -> Dispatcher) shouldBe Some(1.0)
    }
  }

  private def latencyCount(registry: CollectorRegistry, phase: String) =
    sampleValue(registry, "dispatcher_latency_count", "dispatcher" -> Dispatcher, "phase" -> phase)

  private def latencySum(registry: CollectorRegistry, phase: String) =
    sampleValue(registry, "dispatcher_latency_sum", "dispatcher" -> Dispatcher, "phase" -> phase)

  private def workers(registry: CollectorRegistry) =
    sampleValue(registry, "dispatcher_workers", "dispatcher" -> Dispatcher)

  private def sampleValue(registry: CollectorRegistry, name: String, labels: (String, String)*): Option[Double] =
    Option(registry.getSampleValue(name, labels.map(_._1).toArray, labels.map(_._2).toArray)).map(_.doubleValue)
}
