package com.evolutiongaming.cluster

import akka.actor.ActorSystem
import akka.cluster.Cluster

object ClusterOpt {
  def apply(system: ActorSystem): Option[Cluster] = {
    if (system hasExtension Cluster) Some(Cluster(system)) else None
  }
}
