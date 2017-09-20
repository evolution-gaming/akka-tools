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

import akka.actor.{ActorRef, Address}
import akka.cluster.sharding.ShardRegion.ShardId

import scala.collection.immutable
import scala.concurrent.Future

class ExtendedShardAllocationStrategySpec extends AllocationStrategySpec {

  "ExtendedShardAllocationStrategy" should "pass through allocate result if no nodesToDeallocate" in new Scope {

    val strategy = new TestExtendedShardAllocationStrategy() {
      override val nodesToDeallocate = () => Set.empty[Address]
      override val allocateResult  = Some(Refs.head)
    }

    strategy.allocateShard(Refs.last, ShardIds.head, CurrentShardAllocations).futureValue shouldBe Refs.head

    strategy.passedCurrentShardAllocations shouldBe Some(CurrentShardAllocations)
  }

  it should "pass through allocate result if the result not in the list of ignored nodes" in new Scope {

    val strategy = new TestExtendedShardAllocationStrategy() {
      override val nodesToDeallocate = () => Set(testAddress("Address2"), testAddress("Address4"))
      override val allocateResult  = Some(Refs.head)
    }

    strategy.allocateShard(Refs.last, ShardIds.head, CurrentShardAllocations).futureValue shouldBe Refs.head

    strategy.passedCurrentShardAllocations shouldBe Some(CurrentShardAllocations)
  }


  it should "return requester if the result is in the list of ignored nodes" in new Scope {

    val strategy = new TestExtendedShardAllocationStrategy() {
      override val nodesToDeallocate = () => Set(testAddress("Address1"), testAddress("Address4"))
      override val allocateResult  = Some(Refs.head)
    }

    strategy.allocateShard(Refs.last, ShardIds.head, CurrentShardAllocations).futureValue shouldBe Refs.last

    strategy.passedCurrentShardAllocations shouldBe Some(CurrentShardAllocations)
  }

  it should "return arbitrary non-ignored node if the result and requester are both in the list of ignored nodes" in new Scope {

    val strategy = new TestExtendedShardAllocationStrategy() {
      override val nodesToDeallocate = () => Set(testAddress("Address1"), testAddress("Address5"))
      override val allocateResult  = Some(Refs.head)
    }

    strategy.allocateShard(Refs.last, ShardIds.head, CurrentShardAllocations).futureValue should (not equal Refs.head and not equal Refs.last)

    strategy.passedCurrentShardAllocations shouldBe Some(CurrentShardAllocations)
  }

  it  should "pass through rebalance requests if no nodesToDeallocate, limit result size" in new Scope {

    val strategy = new TestExtendedShardAllocationStrategy() {
      override val nodesToDeallocate = () => Set.empty[Address]
      override val rebalanceResult = ShardIds.toSet -- RebalanceInProgress
    }

    strategy.rebalance(CurrentShardAllocations, RebalanceInProgress).futureValue shouldBe
      ((ShardIds.toSet -- RebalanceInProgress) take strategy.maxSimultaneousRebalance - RebalanceInProgress.size)

    strategy.passedCurrentShardAllocations shouldBe Some(CurrentShardAllocations)
    strategy.passedRebalanceInProgress shouldBe Some(RebalanceInProgress)
  }

  it should "rebalance shards from nodesToDeallocate first, limit result size" in new Scope {

    val strategy = new TestExtendedShardAllocationStrategy() {
      override val maxSimultaneousRebalance = 15
      override val nodesToDeallocate = () => Set(testAddress("Address2"), testAddress("Address4"))
      override val rebalanceResult = ShardIds.slice(24, 29).toSet -- RebalanceInProgress
    }

    val mandatoryRebalance = (ShardIds.slice(10, 20) ++ ShardIds.slice(30, 40)).toSet

    strategy.rebalance(CurrentShardAllocations, RebalanceInProgress).futureValue shouldBe
      ((mandatoryRebalance ++ ShardIds.slice(24, 29).toSet --
        RebalanceInProgress) take
        strategy.maxSimultaneousRebalance - RebalanceInProgress.size) -- RebalanceInProgress


    strategy.passedCurrentShardAllocations shouldBe Some(CurrentShardAllocations)
    strategy.passedRebalanceInProgress shouldBe Some(RebalanceInProgress)
  }

  abstract class Scope extends AllocationStrategyScope {

    val ShardIds = for {
      i <- 1 to 50
    } yield s"Shard$i"

    val Refs = Seq(
      mockedHostRef("Address1"),
      mockedHostRef("Address2"),
      mockedHostRef("Address3"),
      mockedHostRef("Address4"),
      mockedHostRef("Address5"))

    val CurrentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]] =
      (for {
        (ref, index) <- Refs.zipWithIndex
      } yield ref -> ShardIds.slice(0 + index * 10, 10 + index * 10)).toMap

    val RebalanceInProgress: Set[ShardId] = Set(
      ShardIds(0),
      ShardIds(10),
      ShardIds(20),
      ShardIds(30),
      ShardIds(40))

    abstract class TestExtendedShardAllocationStrategy extends ExtendedShardAllocationStrategy {

      def allocateResult: Option[ActorRef] = None
      def rebalanceResult: Set[ShardId] = Set.empty
      var passedCurrentShardAllocations: Option[Map[ActorRef, immutable.IndexedSeq[ShardId]]] = None
      var passedRebalanceInProgress: Option[Set[ShardId]] = None

      override val maxSimultaneousRebalance = 10

      protected def doRebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
        rebalanceInProgress: Set[ShardId]): Future[Set[ShardId]] = {

        passedCurrentShardAllocations = Some(currentShardAllocations)
        passedRebalanceInProgress = Some(rebalanceInProgress)
        Future successful rebalanceResult
      }

      protected def doAllocate(
        requester: ActorRef,
        shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {

        passedCurrentShardAllocations = Some(currentShardAllocations)
        Future successful (allocateResult getOrElse requester)
      }
    }
  }
}

