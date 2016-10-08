package com.evolutiongaming.util

import akka.actor._

import scala.collection.immutable.Queue
import scala.compat.Platform
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class BackpressureBuffer[T] private(
  onReceive: (Any, ActorRef) => Option[T],
  onUnbuffer: Queue[T] => Any,
  timeout: FiniteDuration,
  size: Int) extends Actor with ActorLogging {

  import BackpressureBuffer._
  import context.dispatcher

  require(size > 0, s"size must be > 0, but is $size")
  require(timeout > 0.second, s"timeout must be > 0, but is $timeout")

  def receive = receive(Queue.empty, Platform.currentTime, 0)

  def receive(buffer: Queue[T], time: Long, id: Int): Receive = {

    def unbuffer(buffer: Queue[T], timeout: Duration) = {
      log.debug(s"unbuffer when buffer.size: {}, timeout: {}", buffer.size, timeout)
      try onUnbuffer(buffer) catch {
        case NonFatal(e) => log.error(e, "Failed to apply func to {}", buffer)
      }
      context become receive(Queue.empty, Platform.currentTime, id + 1)
    }

    def onEntry(buffer: Queue[T]) = {
      if (buffer.size >= size) {
        unbuffer(buffer, (Platform.currentTime - time).millis.toCoarsest)
      } else {
        if (buffer.size == 1) context.system.scheduler.scheduleOnce(timeout, self, Tick(id))
        context become receive(buffer, time, id)
      }
    }

    {
      case Flush      => if (buffer.nonEmpty) unbuffer(buffer, timeout)
      case Tick(`id`) => if (buffer.nonEmpty) unbuffer(buffer, timeout)
      case Tick(_)    =>
      case entry      => for {entry <- onReceive(entry, sender())} onEntry(buffer enqueue entry)
    }
  }

  private case class Tick(id: Int)
}

object BackpressureBuffer {

  def props[T](
    onReceive: (Any, ActorRef) => Option[T],
    onUnbuffer: Queue[T] => Any,
    timeout: FiniteDuration,
    size: Int): Props = {

    Props(new BackpressureBuffer(onReceive, onUnbuffer, timeout, size))
  }

  def propsByType[T](
    onUnbuffer: Queue[T] => Any,
    timeout: FiniteDuration,
    size: Int)(implicit tag: ClassTag[T]): Props = {

    def onReceive(x: Any, sender: AnyRef) = tag unapply x

    props(onReceive, onUnbuffer, timeout, size)
  }

  case object Flush
}