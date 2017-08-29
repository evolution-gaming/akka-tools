package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class MappedAllocationStrategy(
  typeName: String,
  fallbackStrategy: ShardAllocationStrategy,
  proxy: ActorRef,
  val maxSimultaneousRebalance: Int,
  val nodesToDeallocate: () => Set[Address])(implicit system: ActorSystem, ec: ExecutionContext)
  extends ExtendedShardAllocationStrategy with LazyLogging {

  import MappedAllocationStrategy._

  def mapShardToRegion(shardId: ShardRegion.ShardId, regionRef: ActorRef) =
    proxy ! UpdateMapping(typeName, shardId, regionRef)

  def doAllocate(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    val result = (shardToRegionMapping get EntityKey(typeName, shardId)) flatMap { toNode =>
      val currentRegions = notIgnoredNodes(currentShardAllocations)
      if (currentRegions contains toNode)
        Some(toNode)
      else {
        val toNodeHost = (addressHelper toGlobal toNode.path.address).host
        currentRegions find (ref => (addressHelper toGlobal ref.path.address).host == toNodeHost)
      }
    }

    result match {
      case Some(toNode) =>
        logger debug s"AllocateShard $typeName\n\t" +
          s"shardId:\t$shardId\n\t" +
          s"on node:\t$toNode\n\t" +
          s"requester:\t$requester\n\t"
        Future successful toNode
      case None         =>
        logger debug s"AllocateShard fallback $typeName, shardId:\t$shardId"
        fallbackStrategy.allocateShard(requester, shardId, currentShardAllocations)
    }
  }

  def doRebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    logger debug
      s"doRebalance $typeName: currentShardAllocations = $currentShardAllocations, rebalanceInProgress = $rebalanceInProgress"

    val activeNodes = notIgnoredNodes(currentShardAllocations)

    val shardsToRebalance = for {
      (ref, shards) <- currentShardAllocations
      shardId <- shards if {
      val mappedNode = shardToRegionMapping get EntityKey(typeName, shardId)
      !(mappedNode contains ref) && (mappedNode exists activeNodes.contains)
    }
    } yield shardId

    val result = (shardsToRebalance.toSet -- rebalanceInProgress) take maxSimultaneousRebalance

    if (result.nonEmpty) logger info s"Rebalance $typeName\n\t" +
      s"current:${ currentShardAllocations.mkString("\n\t\t", "\n\t\t", "") }\n\t" +
      s"rebalanceInProgress:\t$rebalanceInProgress\n\t" +
      s"result:\t$result"

    Future successful result
  }
}

object MappedAllocationStrategy {

  def apply(
    typeName: String,
    fallbackStrategy: ShardAllocationStrategy,
    maxSimultaneousRebalance: Int,
    nodesToDeallocate: () => Set[Address])
    (implicit system: ActorSystem, ec: ExecutionContext): MappedAllocationStrategy = {
    // proxy doesn't depend on typeName, it should just start once
    val proxy = MappedAllocationStrategyDDProxy(system).ref
    new MappedAllocationStrategy(
      typeName = typeName,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = maxSimultaneousRebalance,
      nodesToDeallocate = nodesToDeallocate)
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

  case class UpdateMapping(typeName: String, id: ShardRegion.ShardId, regionRef: ActorRef)
  case class Clear(typeName: String, id: ShardRegion.ShardId)

  @volatile
  private[cluster] var shardToRegionMapping: Map[EntityKey, ActorRef] = Map.empty
}