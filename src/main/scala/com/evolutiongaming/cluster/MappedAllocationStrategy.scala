package com.evolutiongaming.cluster

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable
import scala.concurrent.Future

class MappedAllocationStrategy(
  typeName: String,
  fallbackStrategy: ShardAllocationStrategy,
  proxy: ActorRef,
  val maxSimultaneousRebalance: Int)
  extends ShardAllocationStrategy with LazyLogging {

  import MappedAllocationStrategy._

  def mapShardToRegion(shardId: ShardRegion.ShardId, regionRef: ActorRef) =
    proxy ! UpdateMapping(typeName, shardId, regionRef)

  def allocateShard(
    requester: ActorRef,
    shardId: ShardRegion.ShardId,
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]]): Future[ActorRef] = {

    shardToRegionMapping get EntityKey(typeName, shardId) match {
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


  def rebalance(
    currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
    rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] = {

    logger debug
      s"rebalance $typeName: currentShardAllocations = $currentShardAllocations, rebalanceInProgress = $rebalanceInProgress"

    val result = (for {
      (ref, shards) <- currentShardAllocations
      shardId <- shards if !(shardToRegionMapping get EntityKey(typeName, shardId) contains ref)
    } yield shardId).toSet

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
    maxSimultaneousRebalance: Int)
    (implicit system: ActorSystem): MappedAllocationStrategy = {
    // proxy doesn't depend on typeName, it should just start once
    val proxy = MappedAllocationStrategyDistributedDataProxy(system).ref
    new MappedAllocationStrategy(
      typeName = typeName,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = maxSimultaneousRebalance)
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

  // TODO: check for thread-safety
  @volatile
  private[cluster] var shardToRegionMapping: Map[EntityKey, ActorRef] = Map.empty
}