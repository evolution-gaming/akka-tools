package com.evolutiongaming.cluster

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.codahale.metrics.MetricRegistry
import com.github.t3hnar.scalax._

class ClusterEventsMetric(system: ExtendedActorSystem, metricRegistry: MetricRegistry) extends Extension {
  if (system hasExtension Cluster) {
    val listener = system.actorOf(Props(new Listener), classOf[ClusterEventsMetric].simpleName)
    Cluster(system).subscribe(listener, classOf[ClusterDomainEvent])
  }

  class Listener extends Actor {
    def receive = {
      case x: CurrentClusterState =>
      case x: ClusterDomainEvent  => x match {
        case x: MemberEvent       => mark(x, Some(x.member.address))
        case x: LeaderChanged     => mark(x, x.leader)
        case x: RoleLeaderChanged => mark(x, x.leader)
        case x: ReachabilityEvent => x match {
          case x: ReachableMember   => mark(x, Some(x.member.address))
          case x: UnreachableMember => mark(x, Some(x.member.address))
        }
        case x                    =>
          if (x.getClass.simpleName != "ClusterMetricsChanged") mark(x)
      }
    }

    private def mark(event: ClusterDomainEvent, address: Option[Address] = None) = {
      val name = "cluster.events"
      metricRegistry.meter(name).mark()
      metricRegistry.meter(s"$name.${event.getClass.simpleName}").mark()
      for {
        address <- address
        h <- address.host
        port <- address.port
      } {
        val host = h.replace(".", "_")
        metricRegistry.meter(s"$name.${host}_$port.${event.getClass.simpleName}").mark()
      }
    }
  }
}