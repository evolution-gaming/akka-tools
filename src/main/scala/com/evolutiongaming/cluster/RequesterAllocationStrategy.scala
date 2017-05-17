package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.sharding.ShardRegion

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

// Allocate all shards on requester nodes
// to be used primarily for debugging purposes and,
// for example, as fallbackAllocationStrategy in DirectAllocationStrategy
class RequesterAllocationStrategy(
  maxSimultaneousRebalance: Int,
  deallocationTimeout: FiniteDuration)(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy(system, ec, maxSimultaneousRebalance, deallocationTimeout) {

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] =
    Future.successful(requester)

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
    Future.successful(Set.empty)
}
