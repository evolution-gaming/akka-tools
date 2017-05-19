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
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.codahale.metrics.MetricRegistry
import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.collection.{immutable, mutable}
import scala.compat.Platform
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

/**
  * An entity shard initially allocated at a node where the first access to an entity actor occurs.
  * Then entity related messages from clients counted and an entity shard reallocated to a node
  * which receives most of client messages for the corresponding entity
  */
class AdaptiveAllocationStrategy(
  typeName: String,
  rebalanceThresholdPercent: Int,
  cleanupPeriod: FiniteDuration,
  metricRegistry: MetricRegistry,
  countControl: CountControl.Type,
  fallbackStrategy: ShardAllocationStrategy,
  proxy: ActorRef,
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address],
  lowTrafficThreshold: Int = 10)(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy with LazyLogging {

  import AdaptiveAllocationStrategy._

  private implicit val node = Cluster(system)
  private val selfHost = node.selfAddress.host getOrElse "127.0.0.1" replace (".", "_")

  private val cleanupPeriodInMillis = cleanupPeriod.toMillis

  private[cluster] def increase(typeName: String, id: ShardRegion.ShardId, weight: CountControl.Weight): Unit =
    proxy ! Increase(typeName, id, weight)

  private[cluster] def clear(typeName: String, id: ShardRegion.ShardId): Unit = proxy ! Clear(typeName, id)

  /** Should be executed on all nodes, incrementing counters for the local node (side effects only) */
  def wrapExtractShardId(extractShardId: ShardRegion.ExtractShardId): ShardRegion.ExtractShardId = {
    case x: ShardedMsg =>
      val shardId = extractShardId(x)
      val weight = countControl(x)
      if (weight > 0) {
        increase(typeName, shardId, weight)
        if (logger.underlying.isDebugEnabled)
          (metricRegistry meter s"sharding.$typeName.sender.$shardId.$selfHost") mark weight.toLong
      }
      shardId
  }

  /**
    * Allocates the shard on a node with the most client messages received from
    * (or on the requester node if no message statistic have been collected yet).
    * Also should allocate the shard during its rebalance.
    */
  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    def maxOption(nodeCounters: Set[(CounterKey, BigInt)]): Option[(CounterKey, BigInt)] =
      if (nodeCounters.isEmpty) None else Some(nodeCounters maxBy { case (_, cnt) => cnt })

    def maxNode(nodeCounters: Set[(CounterKey, BigInt)]): Option[(CounterKey, BigInt)] = {
      clear(typeName, shardId)
      maxOption(nodeCounters)
    }

    def correctedMaxNode(
      nodeCounters: Set[(CounterKey, BigInt)],
      maxNodeCounterKey: CounterKey,
      maxNodeValue: BigInt): Option[(CounterKey, BigInt)] = {

      // access from a non-home node is counted twice - on the non-home node and on the home node
      // so, to get correct value of the max counter we need to deduct the sum of other counters from it
      // note, that the shard here is deallocated and not present in currentShardAllocations

      val nonMaxNodeCounters = nodeCounters - (maxNodeCounterKey -> maxNodeValue)
      val nonMaxNodeCountersSum = (nonMaxNodeCounters map { case (_, cnt) => cnt }).sum

      val correctedCounters = if (maxNodeValue >= nonMaxNodeCountersSum)
        nonMaxNodeCounters + (maxNodeCounterKey -> (maxNodeValue - nonMaxNodeCountersSum))
      else nodeCounters

      maxOption(correctedCounters)
    }

    def toNode(correctedMaxNodeCounterKey: CounterKey): Option[ActorRef] = {
      val addressFromCounterKey = correctedMaxNodeCounterKey.address
      val correctedMaxNodeAddress = currentShardAllocations.keys find { key =>
        key.toString contains addressFromCounterKey
      }
      correctedMaxNodeAddress
    }

    val entityKey = EntityKey(typeName, shardId)
    val proposedNode = for {
      counterKeys <- entityToNodeCounters get entityKey
      nodeCounters = for {
        counterKey <- counterKeys
        v <- counters get counterKey
      } yield (counterKey, v.value)
      (maxNodeCounterKey, maxNodeValue) <- maxNode(nodeCounters)
      (correctedMaxNodeCounterKey, _) <- correctedMaxNode(nodeCounters, maxNodeCounterKey, maxNodeValue)
      if maxNodeValue >= currentShardAllocations.keys.size
      toNode <- toNode(correctedMaxNodeCounterKey)
    } yield toNode

    proposedNode match {
      case Some(toNode) =>
        logger debug s"AllocateShard $typeName\n\t" +
          s"shardId:\t$shardId\n\t" +
          s"on node:\t$toNode\n\t" +
          s"requester:\t$requester\n\t"
        Future successful toNode
      case None =>
        logger debug s"AllocateShard fallback $typeName, shardId:\t$shardId"
        fallbackStrategy.allocateShard(requester, shardId, currentShardAllocations)
    }
  }

  /** Should be executed every rebalance-interval only on a node with ShardCoordinator */
  protected def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = Future {

    val entityToNodeCountersByType = entityToNodeCounters filterKeys { _.typeName == typeName }

    val shardsToClear = mutable.Set.empty[ShardRegion.ShardId]

    def rebalanceShard(shardId: ShardRegion.ShardId, regionAddress: String): Boolean = {
      val entityKey = EntityKey(typeName, shardId)
      entityToNodeCountersByType get entityKey match {
        case Some(counterKeys) =>
          val cnts = counterKeys flatMap { counterKey =>
            val isHome = counterKey.address == regionAddress
            counters get counterKey map { v =>
              (isHome, v.value, v.cleared)
            }
          }

          val homeValue = cnts collectFirst { case (isHome, v, _) if isHome => v } getOrElse BigInt(0)
          val nonHomeValues = cnts collect { case (isHome, v, _) if !isHome => v }
          val nonHomeValuesSum = nonHomeValues.sum
          val maxNonHomeValue = if (nonHomeValues.isEmpty) BigInt(0) else nonHomeValues.max

          // clear values if needed
          val mostOldPastClear =
            if (cnts.isEmpty) None
            else Some((cnts map { case (_, _, clear) => clear }).min)

          for {
            pastClear <- mostOldPastClear if nonHomeValuesSum > 0 && pastClear < Platform.currentTime - cleanupPeriodInMillis
          } shardsToClear += shardId

          // access from a non-home node is counted twice - on the non-home node and on the home node
          val correctedHomeValue =
            if (homeValue > nonHomeValuesSum) homeValue - nonHomeValuesSum else homeValue
          val rebalanceThreshold =
            (((correctedHomeValue + nonHomeValuesSum) * rebalanceThresholdPercent) / 100) + lowTrafficThreshold

          logger debug s"Shard:$shardId, " +
            s"homeValue:$homeValue, " +
            s"correctedHomeValue:$correctedHomeValue, " +
            s"maxNonHomeValue:$maxNonHomeValue, " +
            s"nonHomeValues:$nonHomeValues, " +
            s"nonHomeValuesSum:$nonHomeValuesSum, " +
            s"rebalanceThreshold:$rebalanceThreshold"

          val rebalance = maxNonHomeValue > correctedHomeValue + rebalanceThreshold
          if (rebalance && logger.underlying.isDebugEnabled) metricRegistry.meter(s"sharding.$typeName.rebalance.$shardId").mark()
          rebalance

        case None => false
      }
    }

    val shardsToRebalance = for {
      (region, regionShards) <- currentShardAllocations
      regionAddress = addressHelper.toGlobal(region.path.address).toString
      notRebalancingShards = regionShards.toSet -- rebalanceInProgress
      shard <- notRebalancingShards if rebalanceShard(shard, regionAddress)
    } yield shard

    val result = shardsToRebalance.toSet

    for (id <- shardsToClear -- result) {
      logger debug s"Shard $typeName#$id counter cleanup"
      clear(typeName, id)
    }

    if (result.nonEmpty) logger info s"Rebalance $typeName\n\t" +
      s"current:${ currentShardAllocations.mkString("\n\t\t", "\n\t\t", "") }\n\t" +
      s"rebalanceInProgress:\t$rebalanceInProgress\n\t" +
      s"result:\t$result"

    result
  }
}

object AdaptiveAllocationStrategy {

  def apply(
    typeName: String,
    rebalanceThresholdPercent: Int,
    cleanupPeriod: FiniteDuration,
    metricRegistry: MetricRegistry,
    countControl: CountControl.Type = CountControl.Empty,
    fallbackStrategy: ShardAllocationStrategy,
    maxSimultaneousRebalance: Int,
    nodesToDeallocate: () => Set[Address])
    (implicit system: ActorSystem, ec: ExecutionContext): AdaptiveAllocationStrategy = {
    // proxy doesn't depend on typeName, it should just start once
    val proxy = AdaptiveAllocationStrategyDistributedDataProxy(system).ref
    new AdaptiveAllocationStrategy(
      typeName = typeName,
      rebalanceThresholdPercent = rebalanceThresholdPercent,
      cleanupPeriod = cleanupPeriod,
      metricRegistry = metricRegistry,
      countControl = countControl,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = maxSimultaneousRebalance,
      nodesToDeallocate = nodesToDeallocate)(system, ec)
  }

  case class EntityKey(typeName: String, id: ShardRegion.ShardId) {
    override def toString: String = s"$typeName#$id"
  }

  object EntityKey {
    def unapply(arg: List[String]): Option[EntityKey] = arg match {
      case typeName :: id :: Nil => Some(EntityKey(typeName, id))
      case _                     => None
    }

    def unapply(arg: String): Option[EntityKey] = unapply((arg split "#").toList)
  }

  case class CounterKey(entityKey: EntityKey, address: String) {
    override def toString: String = s"$entityKey#$address"
  }

  object CounterKey {
    def unapply(arg: String): Option[CounterKey] = (arg split "#").toList match {
      case list :+ address => EntityKey unapply list map { CounterKey(_, address) }
      case _               => None
    }
  }

  case class ValueData(value: BigInt, cleared: Long)

  case class Increase(typeName: String, id: ShardRegion.ShardId, weight: CountControl.Weight)
  case class Clear(typeName: String, id: ShardRegion.ShardId)

  // one access counter per entity-node
  private[cluster] val counters = TrieMap.empty[CounterKey, ValueData]

  // typeName with entity id -> id-s of it's access counters for all nodes
  @volatile
  private[cluster] var entityToNodeCounters: Map[EntityKey, Set[CounterKey]] = Map.empty
}