package com.evolutiongaming.util.dispatchers

import akka.dispatch._
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

/** Instrumented clone of [[akka.dispatch.DispatcherConfigurator]]. */
abstract class AbstractInstrumentedDispatcher(config: Config, prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(config, prerequisites) {

  private val instance = new Dispatcher(
    this,
    config.getString("id"),
    config.getInt("throughput"),
    config.get("throughput-deadline-time"),
    configureExecutor(),
    config.get("shutdown-timeout")
  ) with InstrumentedDispatcherMixin {
    def metricRegistry: MetricRegistry = AbstractInstrumentedDispatcher.this.metricRegistry
  }

  def dispatcher(): MessageDispatcher = instance

  def metricRegistry: MetricRegistry
}
