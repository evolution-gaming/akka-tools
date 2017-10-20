/*
 * Copyright 2009-2017 Lightbend Inc. <http://www.lightbend.com>
 * Copyright 2017 Evolution Gaming Limited
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

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.concurrent.Future

class ExtendedLeastShardAllocationStrategySpec extends AllocationStrategySpec {

  "ExtendedLeastShardAllocationStrategy" should "allocate to region with least number of shards" in new Scope {
    val allocations = Map(region1 -> Vector("Shard1"), region2 -> Vector("Shard2"), region3 -> Vector.empty)
    strategy.allocateShard(region1, "Shard3", allocations).futureValue shouldBe region3
  }

  it should "rebalance from region with most number of shards" in new Scope {
    val allocations = Map(region1 -> Vector("Shard1"), region2 -> Vector("Shard2", "Shard3"),
      region3 -> Vector.empty)

    // region2 has 2 shards and region3 has 0 shards, but the diff is less than rebalanceThreshold
    strategy.rebalance(allocations, Set.empty).futureValue shouldBe Set.empty[String]

    val allocations2 = allocations.updated(region2, Vector("Shard2", "Shard3", "Shard4"))
    strategy.rebalance(allocations2, Set.empty).futureValue shouldBe Set("Shard2", "Shard3")
    strategy.rebalance(allocations2, Set("Shard4")).futureValue shouldBe Set.empty[String]

    val allocations3 = allocations2.updated(region1, Vector("Shard1", "Shard5", "Shard6"))
    strategy.rebalance(allocations3, Set("Shard1")).futureValue shouldBe Set("Shard2")
  }

  it should "rebalance multiple shards if max simultaneous rebalances is not exceeded" in new Scope {
    val allocations = Map(
      region1 -> Vector("Shard1"),
      region2 -> Vector("Shard2", "Shard3", "Shard4", "Shard5", "Shard6"),
      region3 -> Vector.empty)

    strategy.rebalance(allocations, Set.empty).futureValue shouldBe Set("Shard2", "Shard3")
    strategy.rebalance(allocations, Set("Shard2", "Shard3")).futureValue shouldBe Set.empty[String]
  }

  it should "limit number of simultaneous rebalance" in new Scope {
    val allocations = Map(
      region1 -> Vector("Shard1"),
      region2 -> Vector("Shard2", "Shard3", "Shard4", "Shard5", "Shard6"), region3 -> Vector.empty)

    strategy.rebalance(allocations, Set("Shard2")).futureValue shouldBe Set("Shard3")
    strategy.rebalance(allocations, Set("Shard2", "Shard3")).futureValue shouldBe Set.empty[String]
  }

  it should "don't rebalance excessive shards" in new Scope {
    override val strategy = new ExtendedLeastShardAllocationStrategy(
      fallbackStrategy = fallbackStrategy,
      rebalanceThreshold = 1,
      maxSimultaneousRebalance = 5,
      nodesToDeallocate = () => Set(region3.path.address))

    val allocations = Map(
      region1 -> Vector("Shard1", "Shard2", "Shard3", "Shard4", "Shard5", "Shard6", "Shard7"),
      region2 -> Vector("Shard8", "Shard9", "Shard10", "Shard11", "Shard12"),
      region3 -> Vector.empty)

    strategy.rebalance(allocations, Set("Shard1")).futureValue shouldBe Set("Shard2")
    strategy.rebalance(allocations, Set("Shard1", "Shard2")).futureValue shouldBe Set.empty[String]
  }


  abstract class Scope extends AllocationStrategyScope {

    val region1 = mockedHostRef("Address1")
    val region2 = mockedHostRef("Address2")
    val region3 = mockedHostRef("Address3")

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

    val strategy = new ExtendedLeastShardAllocationStrategy(
      fallbackStrategy = fallbackStrategy,
      rebalanceThreshold = 3,
      maxSimultaneousRebalance = 2,
      nodesToDeallocate = () => Set.empty[Address])
  }
}
