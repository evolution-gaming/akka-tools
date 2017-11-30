package com.evolutiongaming.util

import java.lang.management.{ManagementFactory, ThreadInfo}
import java.util.concurrent.Executors

import com.codahale.metrics.MetricRegistry
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.typesafe.scalalogging.LazyLogging

import scala.compat.Platform
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class ExecutionThreadTracker(cache: Cache[ExecutionThreadTracker.ThreadId, ExecutionThreadTracker.StartTime]) {

  def apply[T](f: => T): T = {
    val stop = start()
    try f finally stop()
  }

  def start(): () => Unit = {
    val threadId = Thread.currentThread().getId
    val time = Platform.currentTime
    cache.put(threadId, time)
    () => cache.invalidate(threadId)
  }
}

object ExecutionThreadTracker extends LazyLogging {
  type ThreadId = java.lang.Long
  type StartTime = java.lang.Long

  private implicit lazy val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private lazy val threads = ManagementFactory.getThreadMXBean

  def apply(
    hangingThreshold: FiniteDuration,
    registry: MetricRegistry,
    name: String): ExecutionThreadTracker = {

    val hanging = registry meter s"$name.hanging"
    val hangingDatabase = registry meter s"$name.hanging.database"
    val hangingNonDatabase = registry meter s"$name.hanging.non_database"

    val listener = new RemovalListener[ThreadId, StartTime] {
      def onRemoval(removalNotification: RemovalNotification[ThreadId, StartTime]): Unit = {
        if (removalNotification.wasEvicted()) {
          val threadId = removalNotification.getKey
          val startTime = removalNotification.getValue

          if (Thread.currentThread().getId != threadId) {
            val hangingDuration = Platform.currentTime - startTime

            withThreadInfo(threadId) { threadInfo =>
              val threadName = threadInfo.getThreadName
              val stackTrace = threadInfo.getStackTrace
              hanging.mark()
              if (waitsOnJdbcSocketRead(stackTrace)) {
                logger.warn(s"Hanging for $hangingDuration ms dispatcher thread detected: thread $threadName, waits on JDBC socket read")
                hangingDatabase.mark()
              } else {
                val formattedStackTrace = stackTraceToString(stackTrace)
                logger.error(s"Hanging for $hangingDuration ms dispatcher thread detected: thread $threadName, current state:\t$formattedStackTrace")
                hangingNonDatabase.mark()
              }
            }
          }
        }
      }
    }

    val cache = CacheBuilder.newBuilder()
      .expireAfterWrite(hangingThreshold.length, hangingThreshold.unit)
      .removalListener(listener)
      .build[ThreadId, StartTime]

    new ExecutionThreadTracker(cache)
  }

  def stackTraceToString(xs: Array[StackTraceElement]): String =
    xs.mkString("\tat ", "\n\tat ", "")

  def waitsOnJdbcSocketRead(stackTrace: Array[StackTraceElement]): Boolean =
    stackTrace.exists(_.getClassName.contains("net.sourceforge.jtds.jdbc.SharedSocket"))

  def withThreadInfo(threadId: Long)(f: (ThreadInfo => Unit)): Unit = Future {
    val threadInfo = threads.getThreadInfo(threadId, 300)
    if(threadInfo != null) f(threadInfo)
  }
}