package com.evolutiongaming.util.dispatchers

import com.evolutiongaming.util.dispatchers.Instrumented.Metrics
import io.prometheus.metrics.core.metrics.{Gauge, Summary}
import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.{Unit => PrometheusUnit}

/**
 * [[Instrumented.Metrics.Of]] backed by the Prometheus client 1.x, exposing the same metrics as
 * [[Instrumented.Metrics.Of.apply]] does with the 0.x simpleclient. Its summaries compute quantiles
 * over a 5 minute window instead of 10.
 *
 * It is kept apart from [[Instrumented.Metrics.Of]] because `prometheus-metrics-core` is an
 * optional dependency: only consumers who add it themselves may load classes that reference it.
 */
object PrometheusV1MetricsOf {

  def apply(registry: PrometheusRegistry, prefix: Metrics.Prefix = Metrics.Prefix.Default): Metrics.Of = {

    val latencySummary = Summary
      .builder()
      .name(s"${ prefix }_latency")
      .help("Latency in seconds")
      .labelNames("dispatcher", "phase")
      .quantile(0.9, 0.05)
      .quantile(0.99, 0.005)
      .register(registry)

    val workersGauge = Gauge
      .builder()
      .name(s"${ prefix }_workers")
      .help("Number of workers")
      .labelNames("dispatcher")
      .register(registry)

    // children are resolved lazily, so that a series appears only after the dispatcher has run something
    dispatcher =>
      new Metrics {
        private lazy val workers = workersGauge.labelValues(dispatcher)
        private lazy val queueLatency = latencySummary.labelValues(dispatcher, "queue")
        private lazy val runLatency = latencySummary.labelValues(dispatcher, "run")

        def queue(latencyNanos: Long) = {
          workers.inc()
          queueLatency.observe(PrometheusUnit.nanosToSeconds(latencyNanos))
        }

        def run(latencyNanos: Long) = {
          workers.dec()
          runLatency.observe(PrometheusUnit.nanosToSeconds(latencyNanos))
        }
      }
  }
}
