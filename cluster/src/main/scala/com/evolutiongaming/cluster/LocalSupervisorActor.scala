package com.evolutiongaming.cluster

import akka.actor._
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.Passivate

class LocalSupervisorActor(props: Props, extractEntityId: ShardRegion.ExtractEntityId) extends Actor {
  def receive = {
    case x if extractEntityId.isDefinedAt(x) =>
      val (id, msg) = extractEntityId(x)
      val child = context.child(id) getOrElse (context watch context.actorOf(props, id))
      child forward msg

    case Passivate(x) => sender() ! x

    case Terminated(_) =>
  }
}

object LocalSupervisorActor {
  def props(child: Props, extractEntityId: ShardRegion.ExtractEntityId): Props = {
    def actor = new LocalSupervisorActor(child, extractEntityId)
    Props(actor)
  }
}