package com.evolutiongaming.cluster

import akka.actor.ActorRef
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion

import scala.collection.immutable
import scala.concurrent.Future

// Allocate all shards on requester nodes
// to be used primarily for debugging purposes and,
// for example, as fallbackAllocationStrategy in DirectAllocationStrategy
class RequesterAllocationStrategy extends ShardAllocationStrategy {

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] =
    Future.successful(requester)


  def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
    Future.successful(Set.empty)
}
