package com.evolutiongaming.util

import java.lang.management.ManagementFactory

import akka.actor.ActorSystem
import com.codahale.metrics.{CachedGauge, DerivativeGauge, MetricRegistry}

import scala.concurrent.duration._


class AkkaThreadsMetrics(registry: MetricRegistry, interval: FiniteDuration = 300.millis) {

  type Groups = Map[String, Group]

  case class Group(count: Long, counts: Map[Thread.State, Long])

  def start(system: ActorSystem): Unit = {
    val groups = new ActorSystemGauge(system)
    for {
      (name, group) <- groups.getValue
    } {
      def derivative(count: Group => Long) = new DerivativeGauge[Groups, Long](groups) {
        def transform(groups: Groups): Long = groups get name map count getOrElse 0L
      }

      val count = derivative { _.count }
      registry remove s"$name.threads.all"
      registry.register(s"$name.threads.all", count)

      for {
        state <- Thread.State.values()
      } {
        val count = derivative { _.counts.getOrElse(state, 0L) }
        registry remove s"$name.threads.${ state.toString.toLowerCase }"
        registry.register(s"$name.threads.${ state.toString.toLowerCase }", count)
      }
    }
  }

  class ActorSystemGauge(actorSystem: ActorSystem) extends CachedGauge[Groups](interval.length, interval.unit) {
    private val threads = ManagementFactory.getThreadMXBean
    getValue // eagerly cache value

    def loadValue(): Map[String, Group] = {
      val ids = threads.getAllThreadIds
      val infos = threads.getThreadInfo(ids)

      val systemInfos = for {
        info <- infos if info != null
        name = info.getThreadName if name startsWith actorSystem.name
      } yield info

      for {
        (groupId, infos) <- systemInfos groupBy { info =>
          val array = info.getThreadName split "-"
          array.slice(1, array.size - 1) mkString "-"
        }
      } yield {
        val name = groupId.replace(".", "-")
        val group = Group(
          count = infos.length.toLong,
          counts = infos groupBy { _.getThreadState } map { case (state, list) => state -> list.length.toLong })
        (name, group)
      }
    }
  }
}