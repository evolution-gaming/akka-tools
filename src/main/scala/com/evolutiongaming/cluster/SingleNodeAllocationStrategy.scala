package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardCoordinator.LeastShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContext, Future}

class SingleNodeAllocationStrategy(
  address: => Option[Address],
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy {

  private lazy val leastShardAllocation = new LeastShardAllocationStrategy(
    rebalanceThreshold = 10,
    maxSimultaneousRebalance = maxSimultaneousRebalance)

  protected def doAllocate(requester: ActorRef, shardId: ShardId, current: Map[ActorRef, IndexedSeq[ShardId]]) = {
    val activeNodes = notIgnoredNodes(current)
    def byAddress(address: Address) = activeNodes find { actor => actor.path.address == address }
    def requesterNode = byAddress(requester.path.address)
    val address = this.address
    def masterNode = for {
      a <- address
      n <- byAddress(a)
    } yield n

    val result = masterNode orElse requesterNode

    result map { Future.successful } getOrElse leastShardAllocation.allocateShard(requester, shardId, current)
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