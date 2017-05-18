package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

abstract class ExtendedShardAllocationStrategy(
  implicit system: ActorSystem,
  ec: ExecutionContext) extends ShardAllocationStrategy {

  protected def nodesToDeallocate: () => Set[Address]

  val addressHelper = AddressHelperExtension(system)
  import addressHelper._

  protected def maxSimultaneousRebalance: Int

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]]

  final def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    def limitRebalance(f: => Set[ShardRegion.ShardId]): Set[ShardRegion.ShardId] =
      if (rebalanceInProgress.size >= maxSimultaneousRebalance) Set.empty
      else f take maxSimultaneousRebalance

    val nodesToForcedDeallocation = nodesToDeallocate()

    val shardsToRebalance: Future[Set[ShardRegion.ShardId]] =
      if (nodesToForcedDeallocation.isEmpty) {
        doRebalance(currentShardAllocations, rebalanceInProgress)
      } else {
        val shardsToForcedDeallocation = (for {
          (k, v) <- currentShardAllocations if nodesToForcedDeallocation contains k.path.address.global
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