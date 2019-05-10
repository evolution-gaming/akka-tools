package com.evolutiongaming.util

import java.lang.management.{ManagementFactory, ThreadMXBean}
import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.compat.Platform
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait ExecutionThreadTracker {

  def apply[T](f: => T): T

  def start(): () => Unit
}

object ExecutionThreadTracker extends LazyLogging {

  type ThreadId = java.lang.Long

  type StartTime = java.lang.Long

  private lazy val threads = ManagementFactory.getThreadMXBean
  private lazy val executorService = Executors.newScheduledThreadPool(1)

  def apply(
    hangingThreshold: FiniteDuration,
    checkInterval: FiniteDuration = 1.second,
    maxDepth: Int = 300,
    threads: ThreadMXBean = threads,
    executorService: ScheduledExecutorService = executorService
  ): ExecutionThreadTracker = {

    val cache = TrieMap.empty[ThreadId, StartTime]

    val runnable = new Runnable {
      def run(): Unit = {
        try {
          val currentTime = Platform.currentTime
          for {
            (threadId, startTime) <- cache
            duration = (currentTime - startTime).millis
            if duration >= hangingThreshold
            _ <- cache.remove(threadId)
            threadInfo <- Option(threads.getThreadInfo(threadId, maxDepth))
          } {
            val threadName = threadInfo.getThreadName
            val stackTrace = threadInfo.getStackTrace
            if (waitsOnJdbcSocketRead(stackTrace)) {
              logger.warn(s"Hanging for $duration ms dispatcher thread detected: thread $threadName, waits on JDBC socket read")
            } else {
              val formattedStackTrace = stackTraceToString(stackTrace)
              logger.error(s"Hanging for $duration ms dispatcher thread detected: thread $threadName, current state:\t$formattedStackTrace")
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
      cache.put(threadId, startTime)
      ()
    }

    val remove = (threadId: ThreadId) => {
      cache.remove(threadId)
      ()
    }

    apply(add, remove)
  }

  def apply(
    add: ExecutionThreadTracker.ThreadId => Unit,
    remove: ExecutionThreadTracker.ThreadId => Unit
  ): ExecutionThreadTracker = {

    new ExecutionThreadTracker {

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
  }

  def stackTraceToString(xs: Array[StackTraceElement]): String =
    xs.mkString("\tat ", "\n\tat ", "")

  def waitsOnJdbcSocketRead(stackTrace: Array[StackTraceElement]): Boolean =
    stackTrace.exists(_.getClassName.contains("net.sourceforge.jtds.jdbc.SharedSocket"))
}