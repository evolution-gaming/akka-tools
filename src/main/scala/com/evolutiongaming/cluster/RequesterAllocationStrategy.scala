package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContext, Future}

// Intended to use only as an intermediate fallback allocation strategy
class RequesterAllocationStrategy(
  fallbackStrategy: ShardAllocationStrategy,
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy {

  protected def doAllocate(
    requester: ActorRef,
    shardId: ShardId,
    currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]]): Future[ActorRef] = {

    val activeNodes = notIgnoredNodes(currentShardAllocations)
    def byAddress(address: Address) = activeNodes find { actor => actor.path.address == address }

    byAddress(requester.path.address) match {
      case Some(toNode) => Future successful toNode
      case None         => fallbackStrategy.allocateShard(requester, shardId, currentShardAllocations)
    }
  }

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]],
    rebalanceInProgress: Set[ShardId]): Future[Set[ShardRegion.ShardId]] =
    fallbackStrategy.rebalance(currentShardAllocations, rebalanceInProgress)
}