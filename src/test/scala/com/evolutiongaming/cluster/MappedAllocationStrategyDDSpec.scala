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
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.Replicator.{Changed, Subscribe, Update, WriteLocal}
import akka.cluster.sharding.ShardRegion
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.{Cluster, UniqueAddress}
import akka.testkit.TestProbe
import org.mockito.Mockito._

import scala.collection.immutable
import scala.concurrent.Future

class MappedAllocationStrategyDDSpec extends AllocationStrategySpec {

  import MappedAllocationStrategyDDProxy._

  "MappedAllocationStrategy" should "correctly update its internal mapping" in new Scope {

    strategy.mapShardToRegion(entityId, anotherAddressRef1)

    expectMsgPF() {
      case Update(`MappingKey`, WriteLocal, _) =>
    }

    val mapping = MappedAllocationStrategy.shardToRegionMapping get expectedEntityKey
    mapping.value shouldBe anotherAddressRef1
  }

  it should "correctly process map updates from distributed data" in new Scope {

    MappedAllocationStrategy.shardToRegionMapping should have size 0

    override val map = LWWMap.empty[String, ActorRef] + (expectedEntityKey.toString -> anotherAddressRef2)

    proxy ! Changed(MappingKey)(map)

    eventually {
      val mapping = MappedAllocationStrategy.shardToRegionMapping get expectedEntityKey
      mapping.value shouldBe anotherAddressRef2
    }
  }

  it should "allocate a shard using the fallback strategy if the mapping is empty" in new Scope {

    val requester = TestProbe().testActor

    strategy.allocateShard(
      requester = requester,
      shardId = entityId,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe requester
  }

  it should "allocate a shard on a node (local) by the mapping" in new Scope {

    proxy ! Changed(MappingKey)(map)

    eventually {
      MappedAllocationStrategy.shardToRegionMapping should have size 4
    }

    strategy.allocateShard(
      requester = TestProbe().testActor,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe localAddressRef
  }

  it should "allocate a shard on a node (remote) by the mapping" in new Scope {

    override val map = LWWMap.empty[String, ActorRef] +
      (entityKey1.toString -> anotherAddressRef2) +
      (entityKey2.toString -> anotherAddressRef1) +
      (entityKey3.toString -> anotherAddressRef1) +
      (entityKey4.toString -> localAddressRef)

    proxy ! Changed(MappingKey)(map)

    eventually {
      MappedAllocationStrategy.shardToRegionMapping should have size 4
    }

    strategy.allocateShard(
      requester = TestProbe().testActor,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe anotherAddressRef2
  }

  it should "rebalance shards if there is difference between mapping and current allocation" in new Scope {

    proxy ! Changed(MappingKey)(map)

    eventually {
      MappedAllocationStrategy.shardToRegionMapping should have size 4
    }

    // should rebalance 1,4
    val result1 = strategy.rebalance(
      shardAllocations,
      rebalanceInProgress = Set.empty[ShardId]).futureValue

    result1 shouldBe Set(entityId1, entityId4)

    // 1 is in progress - should rebalance 4
    val result2 = strategy.rebalance(
      shardAllocations,
      rebalanceInProgress = Set[ShardId](entityId1)).futureValue

    result2 shouldBe Set(entityId4)


    // 1, 4 are in progress - should not rebalance
    val result3 = strategy.rebalance(
      shardAllocations,
      rebalanceInProgress = Set[ShardId](entityId4, entityId1)).futureValue

    result3 shouldBe Set()

    // limit rebalance to 1 - should rebalance 1
    val strategy1 = new MappedAllocationStrategy(
      typeName = TypeName,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = 1)

    val result5 = strategy1.rebalance(
      shardAllocations,
      rebalanceInProgress = Set.empty[ShardId]).futureValue

    result5 shouldBe Set(entityId1)
  }

  abstract class Scope extends AllocationStrategyScope {

    val MaxSimultaneousRebalance: Int = 10

    MappedAllocationStrategy.shardToRegionMapping = Map.empty

    val uniqueAddress = UniqueAddress(Address("protocol", "system", "127.0.0.1", 1234), 1L)
    implicit val clusterNode = mock[Cluster]
    when(clusterNode.selfUniqueAddress) thenReturn uniqueAddress
    when(clusterNode.selfAddress) thenReturn uniqueAddress.address

    val TypeName = "typeName"

    val selfAddress = clusterNode.selfAddress.toString

    def entityKey(entityId: String) = MappedAllocationStrategy.EntityKey(TypeName, entityId)

    val entityId = "entityId"
    val expectedEntityKey = entityKey(entityId)
    val entityId1 = "1"
    val entityKey1 = entityKey(entityId1)
    val entityId2 = "2"
    val entityKey2 = entityKey(entityId2)
    val entityId3 = "3"
    val entityKey3 = entityKey(entityId3)
    val entityId4 = "4"
    val entityKey4 = entityKey(entityId4)
    val entityId5 = "5"

    val localAddressRef = mockedAddressRef(clusterNode.selfAddress)
    val anotherAddressRef1 = mockedHostRef("anotherAddress1")
    val anotherAddressRef2 = mockedHostRef("anotherAddress2")

    val anotherAddress1 = anotherAddressRef1.path.address.toString
    val anotherAddress2 = anotherAddressRef2.path.address.toString

    val map = LWWMap.empty[String, ActorRef] +
      (entityKey1.toString -> localAddressRef) +
      (entityKey2.toString -> anotherAddressRef1) +
      (entityKey3.toString -> anotherAddressRef2) +
      (entityKey4.toString -> anotherAddressRef2)

    val shardAllocations = Map[ActorRef, immutable.IndexedSeq[ShardId]](
      anotherAddressRef1 -> immutable.IndexedSeq[ShardId](entityId1, entityId2),
      anotherAddressRef2 -> immutable.IndexedSeq[ShardId](entityId3, entityId5),
      localAddressRef -> immutable.IndexedSeq[ShardId](entityId4))

    val noShard1ShardAllocations = Map[ActorRef, immutable.IndexedSeq[ShardId]](
      anotherAddressRef1 -> immutable.IndexedSeq[ShardId](entityId2),
      anotherAddressRef2 -> immutable.IndexedSeq[ShardId](entityId3),
      localAddressRef -> immutable.IndexedSeq[ShardId](entityId4))

    val proxy = system actorOf Props(new TestProxy)

    val fallbackStrategy = new ExtendedShardAllocationStrategy {
      val maxSimultaneousRebalance = MaxSimultaneousRebalance
      val nodesToDeallocate = () => Set.empty[Address]

      protected def doAllocate(requester: ActorRef, shardId: ShardId,
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] =
        Future successful requester

      override protected def doRebalance(
        currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardRegion.ShardId]],
        rebalanceInProgress: Set[ShardRegion.ShardId]): Future[Set[ShardRegion.ShardId]] =
        Future successful Set.empty
    }

    val strategy = new MappedAllocationStrategy(
      typeName = TypeName,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = MaxSimultaneousRebalance)

    expectMsgPF() {
      case Subscribe(`MappingKey`, _) =>
    }

    class TestProxy extends MappedAllocationStrategyDDProxy {
      override lazy val replicator = testActor
      override implicit lazy val node = clusterNode
    }
  }
}
