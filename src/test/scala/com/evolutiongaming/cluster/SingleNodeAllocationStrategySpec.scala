package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable

class SingleNodeAllocationStrategySpec extends AllocationStrategySpec {

  "SingleNodeAllocationStrategy" should "allocate shards on the master node" in new Scope {
    strategy.allocateShard(mockedHostRef("Address1"), "Shard1", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address3")
    strategy.allocateShard(mockedHostRef("Address2"), "Shard10", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address3")
    strategy.allocateShard(mockedHostRef("Address3"), "Shard30", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address3")
  }

  it should "rebalance shards which are not allocated on the master node" in new Scope {
    strategy.rebalance(CurrentShardAllocations, RebalanceInProgress).futureValue shouldBe Set("Shard31", "Shard41", "Shard32", "Shard11", "Shard2", "Shard12")
  }

  abstract class Scope extends AllocationStrategyScope {

    val ShardIds = for {
      i <- 1 to 50
    } yield s"Shard$i"

    val CurrentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]] = Map(
      mockedHostRef("Address1") -> ShardIds.slice(0, 2),
      mockedHostRef("Address2") -> ShardIds.slice(10, 12),
      mockedHostRef("Address3") -> ShardIds.slice(20, 22),
      mockedHostRef("Address4") -> ShardIds.slice(30, 32),
      mockedHostRef("Address5") -> ShardIds.slice(40, 42))

    val RebalanceInProgress: Set[ShardId] = Set("Shard1")

    val strategy = new SingleNodeAllocationStrategy(
      address = Some(testAddress("Address3")),
      maxSimultaneousRebalance = 7,
      nodesToDeallocate = () => Set.empty[Address])
  }
}
