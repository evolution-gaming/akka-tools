package com.evolutiongaming.cluster

import akka.actor.{ActorSystem, Address}
import akka.cluster.Cluster
import com.evolutiongaming.nel.Nel

import scala.concurrent.TimeoutException

class NodesUnreachableException(addresses: Nel[Address], cause: Throwable) extends TimeoutException {

  override def getCause: Throwable = cause

  override def getMessage: String = addresses match {
    case Nel(node, Nil) => s"node $node is unreachable"
    case nodes          => s"nodes ${ nodes mkString ", " } are unreachable"
  }
}

object NodesUnreachableException {

  def opt(
    timeoutException: TimeoutException,
    system: ActorSystem,
    role: Option[String] = None
  ): Option[NodesUnreachableException] = {

    def unreachableAddresses(cluster: Cluster, role: Option[String]) =
      cluster.state.unreachable.collect { case m if role.forall(m.roles.contains) => m.address }

    for {
      cluster <- ClusterOpt(system)
      unreachable <- Nel.opt(unreachableAddresses(cluster, role))
    } yield new NodesUnreachableException(unreachable, timeoutException)

  }
}