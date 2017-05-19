package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardRegion

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

// Allocate all shards on requester nodes
// to be used primarily for debugging purposes and,
// for example, as fallbackAllocationStrategy in DirectAllocationStrategy
class RequesterAllocationStrategy(
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy {

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] =
    Future successful requester

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
    Future successful Set.empty
}
