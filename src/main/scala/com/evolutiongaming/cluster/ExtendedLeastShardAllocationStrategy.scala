/*
 * Copyright 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright 2017 Evolution Gaming Limited
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

import scala.collection.immutable.IndexedSeq
import scala.concurrent.{ExecutionContext, Future}

class ExtendedLeastShardAllocationStrategy(
  fallbackStrategy: ShardAllocationStrategy,
  val rebalanceThreshold: Int,
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy {

  private val emptyRebalanceResult = Future successful Set.empty[ShardRegion.ShardId]

  protected def doAllocate(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    val activeNodes = notIgnoredNodes(currentShardAllocations)
    val activeAllocations = currentShardAllocations filterKeys activeNodes.contains

    val regionWithLeastShards = if (activeAllocations.isEmpty)
      None
    else Some(
      activeAllocations.toSeq minBy {
        case (_, cnt) => cnt.size
      })

    regionWithLeastShards match {
      case Some((toNode, _)) => Future successful toNode
      case None              => fallbackStrategy.allocateShard(requester, shardId, currentShardAllocations)
    }
  }

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =

    if (rebalanceInProgress.size < maxSimultaneousRebalance) {
      val activeNodes = notIgnoredNodes(currentShardAllocations)
      val activeShards = currentShardAllocations filterKeys activeNodes.contains collect {
        case (_, v) => v filterNot rebalanceInProgress
      }

      if (activeShards.nonEmpty) {
        val leastShards = activeShards minBy (_.size)
        val mostShards = activeShards maxBy (_.size)
        if (mostShards.size - leastShards.size >= rebalanceThreshold)
          Future.successful(mostShards.take(maxSimultaneousRebalance - rebalanceInProgress.size).toSet)
        else
          emptyRebalanceResult
      } else emptyRebalanceResult
    } else emptyRebalanceResult
}
