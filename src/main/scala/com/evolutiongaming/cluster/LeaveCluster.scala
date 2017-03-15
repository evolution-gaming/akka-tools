package com.evolutiongaming.cluster

import akka.actor.{Actor, ActorSystem, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberRemoved
import com.evolutiongaming.util.ConfigHelper._
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

object LeaveCluster {

  def apply(system: ActorSystem, prefix: String = "evolutiongaming.cluster.leave"): Unit = {
    val cluster = Cluster(system)
    if (cluster.state.members.size > 1) {
      val config = system.settings.config getConfig prefix
      apply(cluster, config)
    }
  }

  def apply(cluster: Cluster, config: Config): Unit = {
    val system = cluster.system
    import system.dispatcher

    val gracePeriod = config.get[FiniteDuration]("grace-period")
    val timeout = config.get[FiniteDuration]("timeout")

    val promise = Promise[Unit]()
    val actor = system actorOf Props(new AwaitMemberRemoved(promise, cluster.selfAddress))

    cluster.subscribe(actor, classOf[MemberRemoved])
    cluster leave cluster.selfAddress

    val future = for {
      _ <- Future { Thread sleep gracePeriod.toMillis } // this is needed, believe me
      x <- promise.future
    } yield x

    Try {
      Await.result(future, timeout)
    } recover {
      case e => system.log.warning(s"Error while leaving the cluster $e")
    }
  }

  private class AwaitMemberRemoved(promise: Promise[Unit], address: Address) extends Actor {
    def receive = {
      case MemberRemoved(m, _) =>
        if (m.address == address) {
          promise.trySuccess(())
          context stop self
        }
    }
  }
}
