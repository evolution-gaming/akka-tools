package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContext, Future}

class SingleNodeAllocationStrategy(
  address: => Option[Address],
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy {

  protected def doAllocate(requester: ActorRef, shardId: ShardId, current: Map[ActorRef, IndexedSeq[ShardId]]) = {
    val activeNodes = notIgnoredNodes(current)
    def byAddress(address: Address) = activeNodes find { actor => actor.path.address == address }
    def requesterNode = byAddress(requester.path.address)
    val address = this.address
    def masterNode = for {
      a <- address
      n <- byAddress(a)
    } yield n
    def leastShards(current: Map[ActorRef, IndexedSeq[ShardId]]) = {
      val (regionWithLeastShards, _) = current minBy { case (_, v) => v.size }
      regionWithLeastShards
    }
    def leastShardActive = {
      val currentActiveNodes = current filterKeys { ref => activeNodes contains ref }
      if (currentActiveNodes.isEmpty) None
      else Some(leastShards(currentActiveNodes))
    }

    val result = masterNode orElse requesterNode orElse leastShardActive getOrElse leastShards(current)

    Future successful result
  }

  protected def doRebalance(
    current: Map[ActorRef, IndexedSeq[ShardId]],
    rebalanceInProgress: Set[ShardId]): Future[Set[ShardRegion.ShardId]]= {
    val result = for {
      address <- address.toIterable filterNot nodesToDeallocate().contains
      (actor, shards) <- current if actor.path.address != address
      shard <- shards
    } yield shard

    Future successful result.toSet -- rebalanceInProgress
  }
}