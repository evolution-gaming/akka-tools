package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardCoordinator.LeastShardAllocationStrategy
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future

class SingleNodeAllocationStrategy(
  address: => Option[Address],
  maxSimultaneousRebalance: Int = 10) {

  private lazy val leastShardAllocation = new LeastShardAllocationStrategy(
    rebalanceThreshold = 10,
    maxSimultaneousRebalance = maxSimultaneousRebalance)

  def allocateShard(requester: ActorRef, shardId: ShardId, current: Map[ActorRef, IndexedSeq[ShardId]]) = {
    def byAddress(address: Address) = current.keys.find { actor => actor.path.address == address }
    def requesterNode = byAddress(requester.path.address)
    val address = this.address
    def masterNode = for {
      a <- address
      n <- byAddress(a)
    } yield n

    val result = masterNode orElse requesterNode

    result map { Future.successful } getOrElse leastShardAllocation.allocateShard(requester, shardId, current)
  }

  def rebalance(current: Map[ActorRef, IndexedSeq[ShardId]], rebalanceInProgress: Set[ShardId]) = {
    def limitRebalance(f: => Set[ShardId]): Set[ShardId] = {
      if (rebalanceInProgress.size >= maxSimultaneousRebalance) Set.empty
      else f take maxSimultaneousRebalance
    }

    val result = limitRebalance {
      val result = for {
        address <- address.toIterable
        (actor, shards) <- current if actor.path.address != address
        shard <- shards
      } yield shard
      result.toSet -- rebalanceInProgress
    }

    Future successful result
  }
}