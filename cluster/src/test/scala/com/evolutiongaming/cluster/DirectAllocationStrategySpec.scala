package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.concurrent.Future

class DirectAllocationStrategySpec extends AllocationStrategySpec {

  "DirectAllocationStrategy" should "allocate shards according to the mapping or with the fallback strategy" in new Scope {
    strategy.allocateShard(mockedHostRef("Address1"), "Shard1", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address2")
    strategy.allocateShard(mockedHostRef("Address2"), "Shard2", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address3")
    strategy.allocateShard(mockedHostRef("Address8"), "Shard3", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address4")
    strategy.allocateShard(mockedHostRef("Address9"), "Shard99", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address3")
    strategy.allocateShard(mockedHostRef("Address1"), "Shard100", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address1")
    strategy.allocateShard(mockedHostRef("Address2"), "Shard1111", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address2")
    strategy.allocateShard(mockedHostRef("Address100"), "Shard1111", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address100")
  }

  it should "rebalance shards according to the mapping or with the fallback strategy" in new Scope {
    strategy.rebalance(CurrentShardAllocations, RebalanceInProgress).futureValue shouldBe Set("Shard2", "Shard4", "Shard777")
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

    val RebalanceInProgress: Set[ShardId] = Set("Shard1")

    val fallbackStrategy = new ExtendedShardAllocationStrategy {
      val maxSimultaneousRebalance = 10
      val nodesToDeallocate = () => Set.empty[Address]

      protected def doAllocate(requester: ActorRef, shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] =
        Future successful requester

      override protected def doRebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
        Future successful Set("Shard777")
    }

    val mapping = "Shard1|Address2, Shard2| Address3, Shard3| Address4, Shard4| Address5, Shard99 | Address3, Shard100 |Address6"

    val strategy = DirectAllocationStrategy(
      fallbackStrategy = fallbackStrategy,
      readSettings = () => Some(mapping),
      maxSimultaneousRebalance = 4,
      nodesToDeallocate = () => Set.empty[Address])
  }
}
