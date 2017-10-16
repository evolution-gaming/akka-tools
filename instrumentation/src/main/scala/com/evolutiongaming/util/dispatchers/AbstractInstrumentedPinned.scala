package com.evolutiongaming.util.dispatchers

import akka.dispatch._
import akka.event.Logging.Warning
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.util.ConfigHelper._
import com.typesafe.config.Config

/** Instrumented clone of [[akka.dispatch.PinnedDispatcherConfigurator]]. */
abstract class AbstractInstrumentedPinned(config: Config, prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(config, prerequisites) {

  private val threadPoolConfig: ThreadPoolConfig = configureExecutor() match {
    case e: ThreadPoolExecutorConfigurator ⇒ e.threadPoolConfig
    case _                                 ⇒
      prerequisites.eventStream.publish(
        Warning(
          "PinnedDispatcherConfigurator",
          this.getClass,
          "PinnedDispatcher [%s] not configured to use ThreadPoolExecutor, falling back to default config.".format(
            config.getString("id"))))
      ThreadPoolConfig()
  }

  override def dispatcher(): MessageDispatcher =
    new PinnedDispatcher(
      this, null, config.getString("id"),
      config.get("shutdown-timeout"), threadPoolConfig) with InstrumentedDispatcherMixin {

      def metricRegistry: MetricRegistry = AbstractInstrumentedPinned.this.metricRegistry
    }

  def metricRegistry: MetricRegistry
}
