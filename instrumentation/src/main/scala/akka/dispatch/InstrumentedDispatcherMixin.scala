package akka.dispatch

import akka.event.Logging.Error
import com.evolutiongaming.util.dispatchers.{Instrumented, InstrumentedConfig}

import java.util.concurrent.RejectedExecutionException

trait InstrumentedDispatcherMixin extends Dispatcher {

  def metrics: Instrumented.Metrics.Of

  private lazy val instrumented = {
    val config = InstrumentedConfig(configurator.config)
    Instrumented(config, metrics)
  }

  override def execute(runnable: Runnable): Unit = {
    instrumented(runnable, super.execute)
  }

  /**
   * Clone of [[Dispatcher.executorServiceFactoryProvider]]
   */
  override protected[akka] def registerForExecution(
    mbox: Mailbox,
    hasMessageHint: Boolean,
    hasSystemMessageHint: Boolean,
  ): Boolean = {
    if (mbox.canBeScheduledForExecution(hasMessageHint, hasSystemMessageHint)) { // This needs to be here to ensure thread safety and no races
      if (mbox.setAsScheduled()) {
        try {
          instrumented(mbox, executorService.execute)
          true
        } catch {
          case _: RejectedExecutionException =>
            try {
              instrumented(mbox, executorService.execute)
              true
            } catch { // Retry once
              case e: RejectedExecutionException =>
                mbox.setAsIdle()
                eventStream.publish(Error(e, getClass.getName, getClass, "registerForExecution was rejected twice!"))
                throw e
            }
        }
      } else false
    } else false
  }
}
