package com.evolutiongaming.util

import com.evolutiongaming.util.ExecutionThreadTracker.stackTraceToString
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.BlockContext.withBlockContext
import scala.concurrent.{BlockContext, CanAwait}

object BlockingTracker extends LazyLogging {

  trait Surround {def apply[T](f: () => T): T }

  val Empty: Surround = new Surround {
    override def apply[T](f: () => T): T = f()
  }

  def apply(enabled: Boolean): Surround = {
    if (enabled) {
      new Surround {
        override def apply[T](f: () => T): T = {

          val nonTracking = BlockContext.current

          val tracking = new BlockContext {
            override def blockOn[A](thunk: => A)(implicit permission: CanAwait): A = {

              val thread = Thread.currentThread()
              val stacktrace = stackTraceToString(thread.getStackTrace)
              val name = thread.getName
              logger.warn(s"Thread $name is about to block on: $stacktrace")

              nonTracking.blockOn(thunk)
            }
          }

          withBlockContext(tracking) { f() }
        }
      }
    } else {
      Empty
    }
  }
}