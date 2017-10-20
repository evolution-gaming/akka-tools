/*
 * Copyright 2016-2017 Evolution Gaming Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  protected val emptyRebalanceResult = Future successful Set.empty[ShardRegion.ShardId]

  protected def nodesToDeallocate: () => Set[Address]

  protected val addressHelper = AddressHelperExtension(system)

  protected def maxSimultaneousRebalance: Int

  protected def notIgnoredNodes(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    ignoredNodes: Set[Address] = nodesToDeallocate()): Set[ActorRef] = {
    currentShardAllocations.keySet filterNot { ref =>
      ignoredNodes contains (addressHelper toGlobal ref.path.address)
    }
  }

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
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    val nodeByStrategyFuture = doAllocate(requester, shardId, currentShardAllocations)
    val ignoredNodes = nodesToDeallocate()

    if (ignoredNodes.isEmpty)
      nodeByStrategyFuture
    else {
      for (nodeByStrategy <- nodeByStrategyFuture) yield {

        val activeNodes = notIgnoredNodes(currentShardAllocations, ignoredNodes)
        val activeAddresses = activeNodes map (_.path.address)

        if (activeAddresses contains (addressHelper toGlobal nodeByStrategy.path.address))
          nodeByStrategy
        else if (activeAddresses contains (addressHelper toGlobal requester.path.address))
          requester
        else
          activeNodes.headOption getOrElse currentShardAllocations.keys.head
      }
    }
  }

  final def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    val rebalanceInProgressSize = rebalanceInProgress.size

    if (rebalanceInProgressSize >= maxSimultaneousRebalance)
      emptyRebalanceResult
    else {
      val nodesToForcedDeallocation = nodesToDeallocate()

      def shardsToForcedDeallocation: Set[ShardRegion.ShardId] =
        if (nodesToForcedDeallocation.isEmpty) Set.empty
        else (for {
          (k, v) <- currentShardAllocations if nodesToForcedDeallocation contains (addressHelper toGlobal k.path.address)
        } yield v).flatten.toSet -- rebalanceInProgress

      for {
        doRebalanceResult <- doRebalance(currentShardAllocations, rebalanceInProgress)
        forcedResult = shardsToForcedDeallocation
      } yield {
        val result = forcedResult ++ doRebalanceResult -- rebalanceInProgress
        if (nodesToForcedDeallocation.nonEmpty) logger debug s"Nodes to forcefully deallocate: $nodesToForcedDeallocation"
        if (forcedResult.nonEmpty) logger debug s"Shards to forcefully deallocate: $forcedResult"
        if (result.nonEmpty) logger debug s"Final rebalance result: $result"
        result take maxSimultaneousRebalance - rebalanceInProgressSize
      }
    }
  }
}