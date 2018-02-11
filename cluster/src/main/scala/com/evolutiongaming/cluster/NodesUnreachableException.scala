package com.evolutiongaming.cluster

import akka.actor.{ActorSystem, Address}
import com.evolutiongaming.nel.Nel

import scala.concurrent.TimeoutException

class NodesUnreachableException(addresses: Nel[Address], cause: Throwable) extends TimeoutException {

  override def getCause = cause

  override def getMessage = {
    addresses map { _.hostPort } match {
      case Nel(node, Nil) => s"node $node is unreachable"
      case nodes          => s"nodes ${ nodes mkString ", " } are unreachable"
    }
  }
}

object NodesUnreachableException {

  def opt(timeoutException: TimeoutException, system: ActorSystem): Option[NodesUnreachableException] = {
    for {
      cluster <- ClusterOpt(system)
      members <- Nel.opt(cluster.state.unreachable)
    } yield {
      val addresses = for {member <- members} yield member.address
      new NodesUnreachableException(addresses, timeoutException)
    }
  }
}