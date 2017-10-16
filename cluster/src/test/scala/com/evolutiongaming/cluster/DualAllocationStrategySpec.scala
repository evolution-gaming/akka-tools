package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.concurrent.Future

class DualAllocationStrategySpec extends AllocationStrategySpec {

  "DualAllocationStrategy" should "allocate shards according to the mapping" in new Scope {
    strategy.allocateShard(mockedHostRef("Address1"), "Shard1", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address1")
    strategy.allocateShard(mockedHostRef("Address2"), "Shard2", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address1")
    strategy.allocateShard(mockedHostRef("Address8"), "Shard3", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address1")
    strategy.allocateShard(mockedHostRef("Address9"), "Shard20", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address2")
    strategy.allocateShard(mockedHostRef("Address1"), "Shard21", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address2")
    strategy.allocateShard(mockedHostRef("Address2"), "Shard22", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address2")
  }

  it should "rebalance shards according to the mapping" in new Scope {
    strategy.rebalance(CurrentShardAllocations, RebalanceInProgress).futureValue shouldBe Set("Shard24", "Shard10", "Shard22", "Shard12")
  }

  abstract class Scope extends AllocationStrategyScope {

    val ShardIds = for {
      i <- 1 to 50
    } yield s"Shard$i"

    val CurrentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]] = Map(
      mockedHostRef("Address1") -> ShardIds.slice(0, 10),
      mockedHostRef("Address2") -> ShardIds.slice(10, 20),
      mockedHostRef("Address3") -> ShardIds.slice(20, 30),
      mockedHostRef("Address4") -> ShardIds.slice(30, 40),
      mockedHostRef("Address5") -> ShardIds.slice(40, 50))

    val RebalanceInProgress: Set[ShardId] = Set("Shard11")

    val baseAllocationStrategy = new ExtendedShardAllocationStrategy {
      val maxSimultaneousRebalance = 10
      val nodesToDeallocate = () => Set.empty[Address]

      protected def doAllocate(requester: ActorRef, shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] =
      Future successful mockedHostRef("Address1")

      override protected def doRebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
        Future successful Set("Shard10", "Shard11", "Shard12")
    }

    val additionalAllocationStrategy = new ExtendedShardAllocationStrategy {
      val maxSimultaneousRebalance = 10
      val nodesToDeallocate = () => Set.empty[Address]

      protected def doAllocate(requester: ActorRef, shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] =
        Future successful mockedHostRef("Address2")

      override protected def doRebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
        Future successful Set("Shard22", "Shard23", "Shard24")
    }

    val mapping = "Shard20, Shard21,Shard22, Shard23"

    val strategy = DualAllocationStrategy(
      baseAllocationStrategy = baseAllocationStrategy,
      additionalAllocationStrategy = additionalAllocationStrategy,
      readSettings = () => Some(mapping),
      maxSimultaneousRebalance = 5,
      nodesToDeallocate = () => Set.empty[Address])
  }
}
