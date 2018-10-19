package com.evolutiongaming.cluster

import akka.actor.{ActorSystem, Address}
import com.evolutiongaming.nel.Nel

import scala.concurrent.TimeoutException

class NodesUnreachableException(addresses: Nel[Address], cause: Throwable) extends TimeoutException {

  override def getCause = cause

  override def getMessage = addresses match {
    case Nel(node, Nil) => s"node $node is unreachable"
    case nodes          => s"nodes ${ nodes mkString ", " } are unreachable"
  }
}

object NodesUnreachableException {

  def opt(timeoutException: TimeoutException, system: ActorSystem, role: String): Option[NodesUnreachableException] =
    ClusterOpt(system).flatMap(c =>
      Nel
        .opt(c.state.unreachable.collect { case m if m.roles.contains(role) => m.address })
        .map(new NodesUnreachableException(_, timeoutException))
    )

}