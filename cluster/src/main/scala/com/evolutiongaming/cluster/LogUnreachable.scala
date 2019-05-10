package com.evolutiongaming.cluster

import akka.actor.{Actor, ActorLogging, ExtendedActorSystem, Extension, ExtensionId, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}

import scala.collection.immutable.Queue

class LogUnreachable(system: ExtendedActorSystem) extends Extension {

  private lazy val ref = system.actorOf(Props(new Listener))

  def start(): Unit = { ref; () }


  private class Listener extends Actor with ActorLogging {
    private val cluster = Cluster(context.system)

    private var queue = Queue.empty[ClusterDomainEvent]


    override def preStart() = {
      super.preStart()

      cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[ClusterDomainEvent])
    }

    def receive = {
      case x: ClusterDomainEvent => x match {
        case x: UnreachableMember  => onUnreachableMember(x.member); enqueue(x)
        case x: MemberEvent        => enqueue(x)
        case x: ReachabilityEvent  => enqueue(x)
        case x: LeaderChanged      => enqueue(x)
        case x@ClusterShuttingDown => enqueue(x)
        case _                     =>
      }
    }

    def enqueue(event: ClusterDomainEvent) = {
      queue = queue enqueue event takeRight 20
    }

    def onUnreachableMember(member: Member) = {
      log.warning(s"node ${member.address} is Unreachable, cluster: ${cluster.state}, events: ${queue mkString ","}")
    }
  }
}

object LogUnreachable extends ExtensionId[LogUnreachable] {
  def createExtension(system: ExtendedActorSystem) = new LogUnreachable(system)
}
