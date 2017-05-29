package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

abstract class ExtendedShardAllocationStrategy(
  implicit system: ActorSystem,
  ec: ExecutionContext) extends ShardAllocationStrategy with LazyLogging {

  protected def nodesToDeallocate: () => Set[Address]

  protected val addressHelper = AddressHelperExtension(system)

  protected def maxSimultaneousRebalance: Int

  protected def doAllocate(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef]

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]]

  final def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] =
    doAllocate(requester, shardId, currentShardAllocations)

  final def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    def limitRebalance(f: => Set[ShardRegion.ShardId], rebalanceInProgressSize: Int): Set[ShardRegion.ShardId] =
      if (rebalanceInProgressSize >= maxSimultaneousRebalance) Set.empty
      else f take maxSimultaneousRebalance - rebalanceInProgressSize

    val nodesToForcedDeallocation = nodesToDeallocate()

    def shardsToForcedDeallocation: Set[ShardRegion.ShardId] =
      if (nodesToForcedDeallocation.isEmpty) Set.empty
      else (for {
        (k, v) <- currentShardAllocations if nodesToForcedDeallocation contains (addressHelper toGlobal k.path.address)
      } yield v).flatten.toSet -- rebalanceInProgress

    val result = for {
      doRebalanceResult <- doRebalance(currentShardAllocations, rebalanceInProgress)
      forcedResult = shardsToForcedDeallocation
    } yield {
      val result = forcedResult ++
        limitRebalance(doRebalanceResult, rebalanceInProgress.size + forcedResult.size) -- rebalanceInProgress
      if (nodesToForcedDeallocation.nonEmpty) logger debug s"Nodes to forcefully deallocate: $nodesToForcedDeallocation"
      if (forcedResult.nonEmpty) logger debug s"Shards to forcefully deallocate: $forcedResult"
      if (result.nonEmpty) logger debug s"Final rebalance result: $result"
      result
    }

    result
  }
}