package com.evolutiongaming.cluster

import java.time.Instant

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

abstract class ExtendedShardAllocationStrategy(
  system: ActorSystem,
  implicit val ec: ExecutionContext,
  maxSimultaneousRebalance: Int,
  deallocationTimeout: FiniteDuration) extends ShardAllocationStrategy {

  val addressHelper = AddressHelperExtension(system)
  import addressHelper._

  class Node(val address: Address, added: Instant = Instant.now()) {
    def expired: Boolean = Instant.now() isAfter (added plusMillis deallocationTimeout.toMillis)
    override def equals(obj: Any): Boolean = obj match {
      case node: Node => address equals node.address
      case _          => false
    }
    override def hashCode(): Int = address.hashCode
  }

  @volatile
  private var nodesToForcedDeallocation: Set[Node] = Set.empty

  def deallocateShardsFromNode(regionGlobalAddress: Address): Unit =
    nodesToForcedDeallocation += new Node(regionGlobalAddress)

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]]

  final def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    def limitRebalance(f: => Set[ShardRegion.ShardId]): Set[ShardRegion.ShardId] =
      if (rebalanceInProgress.size >= maxSimultaneousRebalance) Set.empty
      else f take maxSimultaneousRebalance

    val shardsToRebalance: Future[Set[ShardRegion.ShardId]] =
      if (nodesToForcedDeallocation.isEmpty) {
        doRebalance(currentShardAllocations, rebalanceInProgress)
      } else {
        val emptyNodes = for {
          (k, v) <- currentShardAllocations if v.isEmpty
        } yield new Node(k.path.address.global)

        val nodesToRemove = (nodesToForcedDeallocation filter (_.expired)) ++ emptyNodes.toSet

        nodesToForcedDeallocation = nodesToForcedDeallocation -- nodesToRemove

        val shardsToForcedDeallocation = (for {
          (k, v) <- currentShardAllocations if nodesToForcedDeallocation contains new Node(k.path.address.global)
        } yield v).flatten.toSet -- rebalanceInProgress

        for {
          doRebalanceResult <- doRebalance(currentShardAllocations, rebalanceInProgress -- shardsToForcedDeallocation)
        } yield shardsToForcedDeallocation ++ doRebalanceResult
      }

    val result = for {
      shardsToRebalance <- shardsToRebalance
    } yield limitRebalance(shardsToRebalance)

    result
  }
}