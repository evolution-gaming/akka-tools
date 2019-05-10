package akka.dispatch

import java.util.concurrent.RejectedExecutionException

import akka.event.Logging.Error
import com.codahale.metrics.MetricRegistry
import com.evolutiongaming.util.dispatchers.{Instrumented, InstrumentedConfig}

trait InstrumentedDispatcherMixin extends Dispatcher {

  private lazy val instrumented = {
    val config = InstrumentedConfig(configurator.config)
    Instrumented(config, metricRegistry)
  }

  override def execute(runnable: Runnable): Unit = {
    instrumented(runnable, super.execute)
  }

  /**
    * Clone of [[Dispatcher.executorServiceFactoryProvider]]
    */
  protected[akka] override def registerForExecution(
    mbox: Mailbox,
    hasMessageHint: Boolean,
    hasSystemMessageHint: Boolean
  ): Boolean = {
    if (mbox.canBeScheduledForExecution(hasMessageHint, hasSystemMessageHint)) { //This needs to be here to ensure thread safety and no races
      if (mbox.setAsScheduled()) {
        try {
          instrumented(mbox, executorService.execute)
          true
        } catch {
          case _: RejectedExecutionException ⇒
            try {
              instrumented(mbox, executorService.execute)
              true
            } catch { //Retry once
              case e: RejectedExecutionException ⇒
                mbox.setAsIdle()
                eventStream.publish(Error(e, getClass.getName, getClass, "registerForExecution was rejected twice!"))
                throw e
            }
        }
      } else false
    } else false
  }

  def metricRegistry: MetricRegistry
}