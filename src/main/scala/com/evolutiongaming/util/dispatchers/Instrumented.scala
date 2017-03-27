package com.evolutiongaming.util.dispatchers

import akka.dispatch.OverrideAkkaRunnable
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.util.ExecutionThreadTracker
import org.slf4j.MDC

import scala.compat.Platform
import scala.compat.Platform._

class Instrumented(config: InstrumentedConfig, registry: MetricRegistry) {
  import Instrumented._

  private val instruments: List[Instrument] = {
    val name = config.id.replace('.', '-')
    val mdc = if (config.mdc) Some(Instrument.Mdc) else None
    val metrics = if (config.metrics) Some(Instrument.metrics(config.id, name, registry)) else None
    val executionTracker = config.executionTracker map { Instrument.executionTracker(name, _, registry) }
    val result = (mdc ++ metrics ++ executionTracker).toList
    result
  }

  def apply(runnable: Runnable, execute: Runnable => Unit): Unit = {
    val beforeRuns = for {f <- instruments} yield f()
    val run = new Run {
      def apply[T](run: () => T): T = {
        val afterRuns = for {f <- beforeRuns} yield f()
        try run() finally for {f <- afterRuns} f()
      }
    }
    val overridden = OverrideRunnable(runnable, run)
    execute(overridden)
  }

  object OverrideRunnable {

    def apply(runnable: Runnable, r: Run): Runnable = runnable match {
      case OverrideAkkaRunnable(runnable)  => runnable(r)
      case OverrideScalaRunnable(runnable) => runnable(r)
      case runnable                        => new Default(runnable, r)
    }

    class Default(self: Runnable, r: Run) extends Runnable {
      def run(): Unit = r(() => self.run())
    }
  }
}

object Instrumented {
  trait Run { def apply[T](f: () => T): T }

  type Instrument = () => BeforeRun
  type BeforeRun = () => AfterRun
  type AfterRun = () => Unit

  object Instrument {
    val Empty: Instrument = () => () => () => ()

    val Mdc: Instrument = () => {
      val mdc = try MDC.getCopyOfContextMap catch { case e: ConcurrentModificationException => null }
      () => {
        if (mdc == null) MDC.clear() else MDC.setContextMap(mdc)
        () => {
          MDC.clear()
        }
      }
    }

    def metrics(id: String, name: String, registry: MetricRegistry): Instrument = {
      val queue = registry histogram s"$name.queue"
      val run = registry histogram s"$name.run"
      val workers = registry counter s"$name.workers"

      () => {
        val created = Platform.currentTime
        () => {
          val started = Platform.currentTime
          queue update started - created
          workers.inc()
          () => {
            val stopped = Platform.currentTime
            run update stopped - started
            workers.dec()
            ()
          }
        }
      }
    }

    def executionTracker(name: String, config: InstrumentedConfig.ExecutionTracker, registry: MetricRegistry): Instrument = {
      val tracker = ExecutionThreadTracker(config.hangingThreshold, registry, name)
      () => {
        () => {
          val stop = tracker.start()
          () => {
            stop()
            ()
          }
        }
      }
    }
  }
}
