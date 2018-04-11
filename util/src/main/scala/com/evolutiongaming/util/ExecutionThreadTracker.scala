package com.evolutiongaming.util

import java.lang.management.{ManagementFactory, ThreadMXBean}
import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.compat.Platform
import scala.concurrent.duration._
import scala.util.control.NonFatal

class ExecutionThreadTracker(
  add: ExecutionThreadTracker.ThreadId => Unit,
  remove: ExecutionThreadTracker.ThreadId => Unit) {

  def apply[T](f: => T): T = {
    val stop = start()
    try f finally stop()
  }

  def start(): () => Unit = {
    val threadId = Thread.currentThread().getId
    add(threadId)
    () => remove(threadId)
  }
}

object ExecutionThreadTracker extends LazyLogging {
  type ThreadId = java.lang.Long
  type StartTime = java.lang.Long

  private lazy val threads = ManagementFactory.getThreadMXBean
  private lazy val executorService = Executors.newScheduledThreadPool(1)

  def apply(
    hangingThreshold: FiniteDuration,
    checkInterval: FiniteDuration,
    registry: MetricRegistry,
    name: String,
    maxDepth: Int = 300,
    threads: ThreadMXBean = threads,
    executorService: ScheduledExecutorService = executorService): ExecutionThreadTracker = {

    val map = TrieMap.empty[ThreadId, StartTime]

    val hanging = registry meter s"$name.hanging"
    val hangingDatabase = registry meter s"$name.hanging.database"
    val hangingNonDatabase = registry meter s"$name.hanging.non_database"

    val runnable = new Runnable {
      def run(): Unit = {
        try {
          val currentTime = Platform.currentTime
          for {
            (threadId, startTime) <- map
            duration = (currentTime - startTime).millis
            if duration >= hangingThreshold
            _ <- map.remove(threadId)
            threadInfo <- Option(threads.getThreadInfo(threadId, maxDepth))
          } {
            val threadName = threadInfo.getThreadName
            val stackTrace = threadInfo.getStackTrace
            hanging.mark()
            if (waitsOnJdbcSocketRead(stackTrace)) {
              logger.warn(s"Hanging for $duration ms dispatcher thread detected: thread $threadName, waits on JDBC socket read")
              hangingDatabase.mark()
            } else {
              val formattedStackTrace = stackTraceToString(stackTrace)
              logger.error(s"Hanging for $duration ms dispatcher thread detected: thread $threadName, current state:\t$formattedStackTrace")
              hangingNonDatabase.mark()
            }
          }
        } catch {
          case NonFatal(failure) => logger.error(s"failed to check hanging threads: $failure", failure)
        }
      }
    }
    executorService.scheduleWithFixedDelay(runnable, checkInterval.length, checkInterval.length, checkInterval.unit)


    val add = (threadId: ThreadId) => {
      val startTime = Platform.currentTime
      map.put(threadId, startTime)
      ()
    }

    val remove = (threadId: ThreadId) => {
      map.remove(threadId)
      ()
    }

    new ExecutionThreadTracker(add, remove)
  }

  def stackTraceToString(xs: Array[StackTraceElement]): String =
    xs.mkString("\tat ", "\n\tat ", "")

  def waitsOnJdbcSocketRead(stackTrace: Array[StackTraceElement]): Boolean =
    stackTrace.exists(_.getClassName.contains("net.sourceforge.jtds.jdbc.SharedSocket"))
}