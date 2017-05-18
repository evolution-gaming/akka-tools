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

class DualAllocationStrategy(
  baseAllocationStrategy: ShardAllocationStrategy,
  additionalAllocationStrategy: ShardAllocationStrategy,
  readSettings: () => Option[Set[ShardRegion.ShardId]],
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy with LazyLogging {

  @volatile
  private var additionalShardIds = Set.empty[ShardRegion.ShardId]

  for {
    settings <- readSettings()
  } additionalShardIds = settings

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    if (additionalShardIds contains shardId)
      additionalAllocationStrategy.allocateShard(requester, shardId, currentShardAllocations)
    else
      baseAllocationStrategy.allocateShard(requester, shardId, currentShardAllocations)
  }

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    for {
      settings <- readSettings()
    } additionalShardIds = settings

    val currentShardAllocationsOptimized = currentShardAllocations mapValues (_.toSet)

    val additionalStrategyAllocation = currentShardAllocationsOptimized map {
      case (ref, shards) => (ref, (shards intersect additionalShardIds).toIndexedSeq)
    }

    val additionalStrategyResultFuture =
      additionalAllocationStrategy.rebalance(additionalStrategyAllocation, rebalanceInProgress intersect additionalShardIds)

    val baseStrategyAllocation = currentShardAllocationsOptimized map {
      case (ref, shards) => (ref, (shards -- additionalShardIds).toIndexedSeq)
    }

    val baseStrategyResultFuture =
      baseAllocationStrategy.rebalance(baseStrategyAllocation, rebalanceInProgress -- additionalShardIds)

    for {
      additionalStrategyResult <- additionalStrategyResultFuture
      baseStrategyResult <- baseStrategyResultFuture
    } yield baseStrategyResult ++ additionalStrategyResult
  }
}

object DualAllocationStrategy {
  def apply(
    baseAllocationStrategy: ShardAllocationStrategy,
    additionalAllocationStrategy: ShardAllocationStrategy,
    readSettings: () => Option[String],
    maxSimultaneousRebalance: Int,
    nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext): DualAllocationStrategy =
    new DualAllocationStrategy(
      baseAllocationStrategy,
      additionalAllocationStrategy,
      readAndParseSettings(readSettings),
      maxSimultaneousRebalance,
      nodesToDeallocate)

  private def readAndParseSettings(
    readSettings: () => Option[String]): () => Option[Set[ShardRegion.ShardId]] =
    () => for {
      settings <- readSettings()
    } yield (settings split "," map (_.trim) filter (_.nonEmpty)).toSet
}