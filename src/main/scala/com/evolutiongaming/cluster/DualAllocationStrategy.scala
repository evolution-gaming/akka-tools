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

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.sharding.ShardRegion.{ExtractShardId, ShardId}
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.immutable
import scala.concurrent.Future

class DualAllocationStrategy(
  baseAllocationStrategy: ExtendedShardAllocationStrategy,
  additionalAllocationStrategy: ExtendedShardAllocationStrategy,
  readSettings: () => Option[String])(implicit system: ActorSystem) extends ExtendedShardAllocationStrategy
  with LazyLogging {

  import system.dispatcher

  @volatile
  private var additionalShardIds = Set.empty[ShardId]

  reReadSettings() // also frequent calls of rebalance will cause reReadSettings() often

  private def reReadSettings(): Unit =
    for {
      settings <- readSettings()
    } {
      additionalShardIds = (settings split "," map (_.trim) filter (_.nonEmpty)).toSet
    }

  override def extractShardId(numberOfShards: Int): ExtractShardId = {
    case x: ClusterMsg =>
      if (additionalShardIds contains x.id)
        (additionalAllocationStrategy extractShardId numberOfShards) apply x
      else
        (baseAllocationStrategy extractShardId numberOfShards) apply x
  }

  def allocateShard(
    requester: ActorRef,
    shardId: ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {

    reReadSettings()

    if (additionalShardIds contains shardId)
      additionalAllocationStrategy.allocateShard(requester, shardId, currentShardAllocations)
    else
      baseAllocationStrategy.allocateShard(requester, shardId, currentShardAllocations)
  }

  def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
    rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {

    reReadSettings()

    val additionalStrategyAllocation = currentShardAllocations map {
      case (ref, seq) => (ref, seq filter additionalShardIds.contains)
    }
    val additionalStrategyResultFuture =
      additionalAllocationStrategy.rebalance(additionalStrategyAllocation, rebalanceInProgress intersect additionalShardIds)

    val baseStrategyAllocation = currentShardAllocations map {
      case (ref, seq) => (ref, seq filter (id => !(additionalShardIds contains id)))
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
    baseAllocationStrategy: ExtendedShardAllocationStrategy,
    additionalAllocationStrategy: ExtendedShardAllocationStrategy,
    readSettings: () => Option[String])(implicit system: ActorSystem): DualAllocationStrategy =
    new DualAllocationStrategy(baseAllocationStrategy, additionalAllocationStrategy, readSettings)
}
