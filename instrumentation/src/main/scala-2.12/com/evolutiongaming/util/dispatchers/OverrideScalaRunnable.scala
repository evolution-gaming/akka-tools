package com.evolutiongaming.util.dispatchers

import com.evolutiongaming.util.dispatchers.Instrumented.Run

import scala.PartialFunction.condOpt
import scala.concurrent.OnCompleteRunnable

object OverrideScalaRunnable {
  def unapply(runnable: Runnable): Option[Run => Runnable] = condOpt(runnable) {
    case runnable: OnCompleteRunnable => new OverrideOnComplete(runnable, _)
  }

  class OverrideOnComplete(self: Runnable, r: Run) extends OnCompleteRunnable with Runnable {
    def run(): Unit = r(() => self.run())
  }
}
