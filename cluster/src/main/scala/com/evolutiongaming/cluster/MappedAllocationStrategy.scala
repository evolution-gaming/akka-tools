package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionId}
import akka.cluster.ddata.LWWMapKey
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.IndexedSeq
import scala.concurrent.Future

class MappedAllocationStrategy private(
  typeName: String,
  val maxSimultaneousRebalance: Int,
  mapping: MappedAllocationStrategy.Mapping,
  toAddress: ActorRef => Address,
  fallbackStrategy: ShardAllocationStrategy) extends ShardAllocationStrategy with LazyLogging {

  def mapShardToAddress(shardId: ShardRegion.ShardId, address: Address): Unit = {
    mapping.set(shardId, address)
  }

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    current: Map[ActorRef, IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    val region = for {
      address <- mapping get shardId
      region <- current collectFirst { case (region, _) if toAddress(region) == address => region }
    } yield region

    region match {
      case Some(region) =>
        logger debug s"allocateShard $typeName\n\t" +
          s"shardId:\t$shardId\n\t" +
          s"region:\t$region\n\t" +
          s"requester:\t$requester\n\t"
        Future.successful(region)

      case None =>
        logger debug s"allocateShard fallback $typeName, shardId:\t$shardId"
        fallbackStrategy.allocateShard(requester, shardId, current).map { region =>
          mapping.set(shardId, toAddress(region))
          region
        }(CurrentThreadExecutionContext)
    }
  }

  def rebalance(
    current: Map[ActorRef, IndexedSeq[ShardRegion.ShardId]],
    inProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    val toRebalance = for {
      (ref, shards) <- current
      shardId <- shards
      address <- mapping get shardId
      if toAddress(ref) != address
    } yield shardId

    val result = (toRebalance.toSet -- inProgress) take maxSimultaneousRebalance

    if (result.nonEmpty) logger info s"rebalance $typeName\n\t" +
      s"current:${ current.mkString("\n\t\t", "\n\t\t", "") }\n\t" +
      s"inProgress:\t$inProgress\n\t" +
      s"result:\t$result"

    Future successful result
  }
}

object MappedAllocationStrategy {

  def apply(
    typeName: String,
    fallbackStrategy: ShardAllocationStrategy,
    maxSimultaneousRebalance: Int)
    (implicit system: ActorSystem): MappedAllocationStrategy = {

    val mapping = MappingExtension(system).get(typeName)

    val addressHelper = AddressHelperExtension(system)

    new MappedAllocationStrategy(
      typeName,
      maxSimultaneousRebalance,
      mapping,
      (ref: ActorRef) => addressHelper toGlobal ref.path.address,
      fallbackStrategy)
  }


  trait Mapping {
    def get(shardId: ShardRegion.ShardId): Option[Address]
    def set(shardId: ShardRegion.ShardId, address: Address): Unit
  }

  object Mapping {
    def apply(typeName: String, system: ActorSystem): Mapping = {

      val dataKey = LWWMapKey[ShardRegion.ShardId, Address](s"ShardToRegion-$typeName")
      val cache = ReplicatedCache(dataKey, system)

      new Mapping {
        def set(shardId: ShardRegion.ShardId, address: Address): Unit = {
          cache.put(shardId, address)
        }
        def get(shardId: ShardRegion.ShardId): Option[Address] = {
          cache.get(shardId)
        }
      }
    }
  }


  class MappingExtension(system: ActorSystem) extends Extension {
    private val cache = TrieMap.empty[String, Mapping]

    def get(typeName: String): Mapping = {
      cache.getOrElseUpdate(typeName, Mapping(typeName, system))
    }
  }

  object MappingExtension extends ExtensionId[MappingExtension] {
    def createExtension(system: ExtendedActorSystem): MappingExtension = new MappingExtension(system)
  }
}