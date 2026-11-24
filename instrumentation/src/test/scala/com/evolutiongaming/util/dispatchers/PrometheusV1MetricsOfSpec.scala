package com.evolutiongaming.util.dispatchers

import PrometheusV1MetricsOfSpec.Latency
import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.{GaugeSnapshot, MetricSnapshot, SummarySnapshot}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class PrometheusV1MetricsOfSpec extends AnyWordSpec with Matchers {

  private val Dispatcher = "akka-test-dispatcher"

  "PrometheusV1MetricsOf" should {

    "register the same metric names as the 0.x simpleclient variant" in {
      val registry = new PrometheusRegistry()
      PrometheusV1MetricsOf(registry)

      registry.scrape().asScala.map(_.getMetadata.getPrometheusName).toSet shouldBe Set(
        "dispatcher_latency",
        "dispatcher_workers",
      )
    }

    "not expose dispatcher series before the dispatcher has run anything" in {
      val registry = new PrometheusRegistry()
      PrometheusV1MetricsOf(registry).apply(Dispatcher)

      latency(registry, "queue") shouldBe None
      workers(registry) shouldBe None
    }

    "observe queue and run latency in seconds and track busy workers" in {
      val registry = new PrometheusRegistry()
      val metrics = PrometheusV1MetricsOf(registry).apply(Dispatcher)

      metrics.queue(latencyNanos = 2000000L)
      workers(registry) shouldBe Some(1.0)

      metrics.run(latencyNanos = 3000000L)
      workers(registry) shouldBe Some(0.0)

      latency(registry, "queue") shouldBe Some(Latency(count = 1, sum = 0.002))
      latency(registry, "run") shouldBe Some(Latency(count = 1, sum = 0.003))
    }

    "expose the 0.9 and 0.99 quantiles" in {
      val registry = new PrometheusRegistry()
      PrometheusV1MetricsOf(registry).apply(Dispatcher).queue(latencyNanos = 1000000L)

      val quantiles = summaryDataPoints(registry, "dispatcher_latency")
        .flatMap(_.getQuantiles.asScala.map(_.getQuantile))
        .toSet

      quantiles shouldBe Set(0.9, 0.99)
    }

    "use the given prefix" in {
      val registry = new PrometheusRegistry()
      PrometheusV1MetricsOf(registry, prefix = "custom").apply(Dispatcher).queue(latencyNanos = 1000000L)

      workers(registry, metricName = "custom_workers") shouldBe Some(1.0)
    }
  }

  private def latency(registry: PrometheusRegistry, phase: String): Option[Latency] =
    summaryDataPoints(registry, "dispatcher_latency")
      .find(dataPoint => dataPoint.getLabels.get("phase") == phase)
      .map(dataPoint => Latency(count = dataPoint.getCount, sum = dataPoint.getSum))

  private def workers(registry: PrometheusRegistry, metricName: String = "dispatcher_workers"): Option[Double] =
    snapshot(registry, metricName)
      .collect { case gauge: GaugeSnapshot => gauge.getDataPoints.asScala.toList }
      .getOrElse(Nil)
      .find(dataPoint => dataPoint.getLabels.get("dispatcher") == Dispatcher)
      .map(_.getValue)

  private def summaryDataPoints(registry: PrometheusRegistry, metricName: String) =
    snapshot(registry, metricName)
      .collect { case summary: SummarySnapshot => summary.getDataPoints.asScala.toList }
      .getOrElse(Nil)
      .filter(dataPoint => dataPoint.getLabels.get("dispatcher") == Dispatcher)

  private def snapshot(registry: PrometheusRegistry, metricName: String): Option[MetricSnapshot] =
    registry.scrape().asScala.find(_.getMetadata.getPrometheusName == metricName)
}

object PrometheusV1MetricsOfSpec {

  final case class Latency(count: Long, sum: Double)
}
