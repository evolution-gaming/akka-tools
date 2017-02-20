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

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion._
import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.collection.{immutable, mutable}
import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * An entity shard initially allocated at a node where the first access to an entity actor occurs.
  * Then entity related messages from clients counted and an entity shard reallocated to a node
  * which receives most of client messages for the corresponding entity
  */
class AdaptiveAllocationStrategy(
  typeName: String,
  system: ActorSystem,
  maxSimultaneousRebalance: Int,
  rebalanceThreshold: Int,
  cleanupPeriod: FiniteDuration,
  metricRegistry: MetricRegistry)
  extends ExtendedShardAllocationStrategy with LazyLogging {

  import AdaptiveAllocationStrategy._
  import system.dispatcher

  private implicit val node = Cluster(system)
  private val selfAddress = node.selfAddress.toString
  private val selfHost = node.selfAddress.host getOrElse "127.0.0.1" replace (".", "_")

  private val cleanupPeriodInMillis = cleanupPeriod.toMillis

  /** Should be executed on all nodes, incrementing counters for the local node */
  override def extractShardId(numberOfShards: Int): ShardRegion.ExtractShardId = {
    case x: ClusterMsg =>
      if (!x.isInstanceOf[PersistenceQuery]) {
        x match {
          case cc: CountControl if cc.doNotCount =>
          case _                                 =>
            increment(typeName, x.id)
            metricRegistry.meter(s"persistence.$typeName.sender.${ x.id }.$selfHost").mark()
        }
        metricRegistry.meter(s"persistence.$typeName.command.${ x.id }.${x.getClass.getSimpleName}.$selfHost").mark()
      }
      x.id
  }

  /**
    * Allocates the shard on a node with the most client messages received from
    * (or on the requester node if no message statistic have been collected yet).
    * Also should allocate the shard during its rebalance.
    */
  def allocateShard(
    requester: ActorRef,
    shardId: ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {

    val entityKey = genEntityKey(typeName, shardId)
    val toNode = entityToNodeCounters get entityKey match {
      case None              => requester
      case Some(counterKeys) =>
        val nodeCounters = for {
          counterKey <- counterKeys
          v <- counters get counterKey
        } yield (counterKey, v.value)

        val maxNode = nodeCounters.reduceOption[(String, BigInt)] {
          case ((key1, cnt1), (key2, cnt2)) => if (cnt1 >= cnt2) (key1, cnt1) else (key2, cnt2)
        }

        maxNode match {
          case None                  => requester
          case Some((counterKey, _)) =>
            val addressFromCounterKey = addressByCounterKey(counterKey)
            val maxNodeAddress = currentShardAllocations.keys find { key =>
              key.toString contains addressFromCounterKey
            }
            maxNodeAddress getOrElse requester
        }
    }

    logger.debug(
      s"AllocateShard $typeName\n\t" +
        s"on node:\t$toNode\n\t" +
        s"requester:\t$requester\n\t" +
        s"shardId:\t$shardId\n\t")
    Future successful toNode
  }

  /** Should be executed every rebalance-interval only on a node with ShardCoordinator */
  def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
    rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = Future {

    def limitRebalance(f: => Set[ShardId]): Set[ShardId] = {
      if (rebalanceInProgress.size >= maxSimultaneousRebalance) Set.empty
      else f take maxSimultaneousRebalance
    }

    val entityToNodeCountersByType = entityToNodeCounters filterKeys (_ startsWith typeName)

    val shardsToClear = mutable.Set.empty[ShardId]

    val shardsToRebalance = currentShardAllocations flatMap { case (region, regionShards) =>
      val regionAddress = if(region.path.address.hasGlobalScope) region.path.address.toString else selfAddress
      val notRebalancingShards = regionShards.toSet diff rebalanceInProgress

      notRebalancingShards flatMap { shard =>
        val entityKey = genEntityKey(typeName, shard)
        entityToNodeCountersByType get entityKey flatMap { counterKeys =>
          val cnts = counterKeys flatMap { counterKey =>
            val isHome = counterKey endsWith regionAddress
            counters get counterKey map { v =>
              (isHome, v.value, v.cleared)
            }
          }

          val homeValue =
            cnts find { case (isHome, _, _) => isHome } map { case (_, v, _)  => v } getOrElse BigInt(0)
          val nonHomeValues = cnts collect { case (isHome, v, _) if !isHome => v }
          val nonHomeValuesSum = nonHomeValues.sum
          val maxNonHomeValue = nonHomeValues reduceOption ((x, y) => if (x >= y) x else y) getOrElse BigInt(0)

          // clear values if needed
          val mostOldPastClear = cnts map { case (_, _, clear) => clear } reduceOption
            ((x, y) => if (x <= y) x else y)
          for (pastClear <- mostOldPastClear
               if nonHomeValuesSum > 0 && pastClear < Platform.currentTime - cleanupPeriodInMillis) {
            shardsToClear += shard
          }

          // access from a non-home node is counted twice - on the non-home node and on the home node
          if (maxNonHomeValue > homeValue - nonHomeValuesSum + rebalanceThreshold) {
            shardsToClear += shard
            metricRegistry.meter(s"persistence.$typeName.rebalance.$shard").mark()
            Some(shard)
          } else None
        }
      }
    }

    val result = limitRebalance(shardsToRebalance.toSet)

    for (id <- shardsToClear) {
      logger.debug(s"Shard $typeName#$id counter cleanup")
      clear(typeName, id)
    }

    if (result.nonEmpty) logger.info(
      s"Rebalance $typeName\n\t" +
        s"current:${currentShardAllocations.mkString("\n\t\t", "\n\t\t", "")}\n\t" +
        s"rebalanceInProgress:\t$rebalanceInProgress\n\t" +
        s"result:\t$result")

    result
  }
}

object AdaptiveAllocationStrategy {

  // proxyProps is needed for unit tests
  def apply(
    typeName: String,
    maxSimultaneousRebalance: Int,
    rebalanceThreshold: Int,
    cleanupPeriod: FiniteDuration,
    metricRegistry: MetricRegistry)
    (proxyProps: Props = Props[AdaptiveAllocationStrategyDistributedDataProxy])
    (implicit system: ActorSystem): AdaptiveAllocationStrategy = {
    // proxy doesn't depend on typeName, it should just start once
    if (proxy.isEmpty) this synchronized {
      if (proxy.isEmpty) proxy = Some(system actorOf proxyProps)
    }
    new AdaptiveAllocationStrategy(
      typeName = typeName,
      system = system,
      maxSimultaneousRebalance = maxSimultaneousRebalance,
      rebalanceThreshold = rebalanceThreshold,
      cleanupPeriod = cleanupPeriod,
      metricRegistry: MetricRegistry)
  }

  def genEntityKey(typeName: String, id: ShardId): String =
    s"$typeName#$id" // should always start with typeName
  def genCounterKey(entityKey: String, selfAddress: String): String =
    s"$entityKey#$selfAddress" // should always end with selfAddress
  def addressByCounterKey(address: String): String =
    (address split "#") lift 2 getOrElse "" // typeName#shardId#address

  @volatile
  private[cluster] var proxy: Option[ActorRef] = None

  // one access counter per entity-node
  private[cluster] val counters = TrieMap.empty[String, ValueData]

  // typeName with entity id -> id-s of it's access counters for all nodes
  @volatile
  private[cluster] var entityToNodeCounters: Map[String, Set[String]] = Map.empty

  private[cluster] def increment(typeName: String, id: ShardId): Unit =
    proxy foreach (_ ! Increment(typeName, id))

  private[cluster] def clear(typeName: String, id: ShardId): Unit =
    proxy foreach (_ ! Clear(typeName, id))

  case class ValueData(value: BigInt, cleared: Long)

  case class Increment(typeName: String, id: ShardId)
  case class Clear(typeName: String, id: ShardId)
}