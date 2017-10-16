package com.evolutiongaming.cluster

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.concurrent.Future

class RequesterAllocationStrategySpec extends AllocationStrategySpec {

  "RequesterAllocationStrategy" should "allocate shards on the requester node" in new Scope {
    strategy.allocateShard(mockedHostRef("Address1"), "Shard1", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address1")
    strategy.allocateShard(mockedHostRef("Address2"), "Shard10", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address2")
    strategy.allocateShard(mockedHostRef("Address3"), "Shard30", CurrentShardAllocations).futureValue shouldBe mockedHostRef("Address5")
  }

  it should "proxy rebalance result with deallocation shards from nodesToDeallocate " in new Scope {
    strategy.rebalance(CurrentShardAllocations, RebalanceInProgress).futureValue shouldBe
      Set("Shard31", "Shard41", "Shard21", "Shard32", "Shard22")
  }

  abstract class Scope extends AllocationStrategyScope {

    val MaxSimultaneousRebalance = 7

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

    val fallbackStrategy = new ExtendedShardAllocationStrategy {
      val maxSimultaneousRebalance = MaxSimultaneousRebalance
      val nodesToDeallocate = () => Set.empty[Address]

      protected def doAllocate(requester: ActorRef, shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] =
        Future successful mockedHostRef("Address5")

      override protected def doRebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
        Future successful Set("Shard31", "Shard41", "Shard32")
    }

    val strategy = new RequesterAllocationStrategy(
      fallbackStrategy = fallbackStrategy,
      maxSimultaneousRebalance = MaxSimultaneousRebalance,
      nodesToDeallocate = () => Set(testAddress("Address3")))
  }
}
