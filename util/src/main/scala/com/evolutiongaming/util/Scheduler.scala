package com.evolutiongaming.util

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Gives ability to decide whether to run scheduled task on system termination by setting `runOnShutdown`
  */
@deprecated("Use akka.actor.Scheduler directly, check https://doc.akka.io/docs/akka/2.6/project/migration-guide-2.5.x-2.6.x.html#scheduler-not-running-tasks-when-shutdown", "3.0.11")
class Scheduler(scheduler: akka.actor.Scheduler) extends Extension { self =>

  private val shuttingDown = new AtomicBoolean(false)

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    runOnShutdown: Boolean)
    (f: => Unit)
    (implicit executor: ExecutionContext): Cancellable = {

    def runnable = new Runnable {
      def run() = self.run(runOnShutdown, f)
    }
    
    scheduler.scheduleWithFixedDelay(initialDelay, interval)(runnable)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration)
    (f: => Unit)
    (implicit executor: ExecutionContext): Cancellable = {

    schedule(initialDelay, interval, runOnShutdown = true)(f)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    receiver: ActorRef,
    message: Any,
    runOnShutdown: Boolean)
    (implicit executor: ExecutionContext, sender: ActorRef): Cancellable = {

    schedule(initialDelay, interval, runOnShutdown)(receiver.tell(message, sender))
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    receiver: ActorRef,
    message: Any)
    (implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {

    schedule(initialDelay, interval, receiver, message, runOnShutdown = true)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    runnable: Runnable,
    runOnShutdown: Boolean)
    (implicit executor: ExecutionContext): Cancellable = {

    schedule(initialDelay, interval, runOnShutdown)(runnable.run())
  }

  def schedule(
    initialDelay: FiniteDuration,
    interval: FiniteDuration,
    runnable: Runnable)
    (implicit executor: ExecutionContext): Cancellable = {

    schedule(initialDelay, interval, runnable, runOnShutdown = true)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def scheduleOnce(delay: FiniteDuration, runOnShutdown: Boolean)
    (f: => Unit)
    (implicit executor: ExecutionContext): Cancellable = {

    scheduler.scheduleOnce(delay)(run(runOnShutdown, f))
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def scheduleOnce(delay: FiniteDuration)
    (f: => Unit)
    (implicit executor: ExecutionContext): Cancellable = {

    scheduleOnce(delay, runOnShutdown = true)(f)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def scheduleOnce(
    delay: FiniteDuration,
    receiver: ActorRef,
    message: Any,
    runOnShutdown: Boolean)
    (implicit executor: ExecutionContext, sender: ActorRef): Cancellable = {

    scheduleOnce(delay, runOnShutdown)(receiver.tell(message, sender))
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def scheduleOnce(
    delay: FiniteDuration,
    receiver: ActorRef,
    message: Any)
    (implicit executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {

    scheduleOnce(delay, receiver, message, runOnShutdown = true)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def scheduleOnce(
    delay: FiniteDuration,
    runnable: Runnable,
    runOnShutdown: Boolean)
    (implicit executor: ExecutionContext): Cancellable = {

    scheduleOnce(delay, runOnShutdown)(runnable.run())
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def scheduleOnce(
    delay: FiniteDuration,
    runnable: Runnable)
    (implicit executor: ExecutionContext): Cancellable = {

    scheduleOnce(delay, runnable, runOnShutdown = true)
  }

  @deprecated("Use akka.actor.Scheduler directly", "3.0.11")
  def shutdown(): Unit = {
    shuttingDown.set(true)
  }

  private def run(runOnShutdown: Boolean, f: => Unit) = {
    if (!shuttingDown.get() || runOnShutdown) f
  }
}

object Scheduler extends ExtensionId[Scheduler] {
  def createExtension(system: ExtendedActorSystem) = new Scheduler(system.scheduler)
}
