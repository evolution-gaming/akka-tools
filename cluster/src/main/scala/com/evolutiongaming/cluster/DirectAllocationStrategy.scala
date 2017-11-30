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

class DirectAllocationStrategy(
  fallbackStrategy: ShardAllocationStrategy,
  readSettings: () => Option[Map[ShardRegion.ShardId, String]],
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy with LazyLogging {

  @volatile
  private var shardIdToAddress = Map.empty[ShardRegion.ShardId, String]

  for {
    settings <- readSettings()
  } shardIdToAddress = settings

  private def addressForShardId(
    shardId: ShardRegion.ShardId,
    addresses: Set[ActorRef]): Option[ActorRef] = {

    shardIdToAddress get shardId flatMap { ipAddress =>
      addresses find (ref => (addressHelper toGlobal ref.path.address).host contains ipAddress)
    }
  }

  protected def doAllocate(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    val activeNodes = notIgnoredNodes(currentShardAllocations)
    val targetAddress = addressForShardId(shardId, activeNodes)

    targetAddress match {
      case Some(address) =>
        logger debug s"Allocate shardId:\t$shardId\t" +
          s"on node:\t$address\t" +
          s"requester:\t$requester\t"
        Future successful address
      case None =>
        fallbackStrategy.allocateShard(requester, shardId, currentShardAllocations)
    }
  }

  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    for {
      settings <- readSettings()
    } shardIdToAddress = settings

    val ourShards = shardIdToAddress.keySet
    val activeNodes = notIgnoredNodes(currentShardAllocations)
    val currentShardAllocationsOptimized = currentShardAllocations mapValues (_.toSet)

    val shardsToReallocate = for {
      shardId <- ourShards
      targetAddress <- addressForShardId(shardId, activeNodes)
      currentAddress <- currentShardAllocationsOptimized collectFirst {
        case (address, shards) if shards contains shardId => address
      }
      if (addressHelper toGlobal currentAddress.path.address) != (addressHelper toGlobal targetAddress.path.address)
    } yield shardId

    val fallbackStrategyAllocation = currentShardAllocationsOptimized map {
      case (ref, shards) => (ref, (shards -- ourShards).toIndexedSeq)
    }

    for {
      fallbackStrategyResult <- fallbackStrategy.rebalance(fallbackStrategyAllocation, rebalanceInProgress -- ourShards)
    } yield {
      val result = shardsToReallocate ++ fallbackStrategyResult -- rebalanceInProgress
      if (result.nonEmpty) logger info s"Rebalance\t" +
        s"current:${ currentShardAllocations.mkString("\t\t", "\t\t", "") }\t" +
        s"shardsToReallocate:\t$shardsToReallocate\t" +
        s"fallbackStrategyResult:\t$fallbackStrategyResult\t" +
        s"rebalanceInProgress:\t$rebalanceInProgress\t" +
        s"result:\t$result"
      result
    }
  }
}

object DirectAllocationStrategy {
  def apply(
    fallbackStrategy: ShardAllocationStrategy,
    readSettings: () => Option[String],
    maxSimultaneousRebalance: Int,
    nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext): DirectAllocationStrategy =
    new DirectAllocationStrategy(
      fallbackStrategy,
      readAndParseSettings(readSettings),
      maxSimultaneousRebalance,
      nodesToDeallocate)

  // entityId|ipAddress, entityId | ipAddress, entityId|ipAddress
  private def readAndParseSettings(
    readSettings: () => Option[String]): () => Option[Map[ShardRegion.ShardId, String]] =
    () => for {
      settings <- readSettings()
    } yield {
      ((settings split "," map (_.trim) filter (_.nonEmpty)) map { elem =>
        (elem split "\\|" map (_.trim) filter (_.nonEmpty)).toList
      } collect {
        case k :: v :: Nil => (k, v)
      }).toMap
    }
}