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
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.Future

// this AllocationStrategy is for debug purposes only
class DirectAllocationStrategy(
  fallbackStrategy: ShardAllocationStrategy,
  readSettings: () => Option[Map[ShardRegion.ShardId, String]])
  (implicit system: ActorSystem) extends ShardAllocationStrategy
  with LazyLogging {

  import system.dispatcher

  val addressHelper = AddressHelperExtension(system)
  import addressHelper._

  @volatile
  private var shardIdToAddress = Map.empty[ShardRegion.ShardId, String]

  for {
    settings <- readSettings()
  } shardIdToAddress = settings

  private def addressForShardId(
    shardId: ShardRegion.ShardId,
    addresses: Set[ActorRef]): Option[ActorRef] = {

    shardIdToAddress get shardId flatMap { ipAddress =>
      addresses find (_.path.address.global.host contains ipAddress)
    }
  }

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    val addresses = currentShardAllocations.keySet + requester
    val targetAddress = addressForShardId(shardId, addresses)

    targetAddress match {
      case Some(address) =>
        logger debug s"Allocate shardId:\t$shardId\n\t" +
          s"on node:\t$address\n\t" +
          s"requester:\t$requester\n\t"
        Future successful address
      case None =>
        fallbackStrategy.allocateShard(requester, shardId, currentShardAllocations)
    }
  }

  def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    for {
      settings <- readSettings()
    } shardIdToAddress = settings

    val ourShards = shardIdToAddress.keySet
    val currentShardAllocationsOptimized = currentShardAllocations mapValues (_.toSet)

    val shardsToReallocate = for {
      shardId <- ourShards
      targetAddress <- addressForShardId(shardId, currentShardAllocations.keySet)
      currentAddress = currentShardAllocationsOptimized collectFirst {
        case (address, shards) if shards contains shardId => address
      }
      if !(currentAddress contains targetAddress)
    } yield shardId

    val fallbackStrategyAllocation = currentShardAllocationsOptimized map {
      case (ref, shards) => (ref, (shards -- ourShards).toIndexedSeq)
    }

    for {
      fallbackStrategyResult <- fallbackStrategy.rebalance(fallbackStrategyAllocation, rebalanceInProgress -- ourShards)
    } yield shardsToReallocate ++ fallbackStrategyResult
  }
}

object DirectAllocationStrategy {
  def apply(
    fallbackStrategy: ShardAllocationStrategy,
    readSettings: () => Option[String])(implicit system: ActorSystem): DirectAllocationStrategy =
    new DirectAllocationStrategy(fallbackStrategy, readAndParseSettings(readSettings))

  private def readAndParseSettings(
    readSettings: () => Option[String]): () => Option[Map[ShardRegion.ShardId, String]] =
  // entityId|ipAddress, entityId | ipAddress, entityId|ipAddress
    () => for {
      settings <- readSettings()
    } yield {
      ((settings split "," map (_.trim) filter (_.nonEmpty)) map { elem =>
        (elem split "|" map (_.trim) filter (_.nonEmpty)).toList
      } collect {
        case k :: v :: Nil => (k, v)
      }).toMap
    }
}