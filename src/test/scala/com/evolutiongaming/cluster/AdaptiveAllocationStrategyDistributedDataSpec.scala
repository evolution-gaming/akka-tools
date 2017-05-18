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

import akka.TestDummyActorRef
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.{Changed, Subscribe, Update, WriteLocal}
import akka.cluster.ddata.{ORMultiMap, PNCounter, PNCounterKey}
import akka.cluster.sharding.ShardRegion.ShardId
import akka.testkit.{DefaultTimeout, TestProbe}
import com.codahale.metrics.{Meter, MetricRegistry}
import com.evolutiongaming.cluster.AdaptiveAllocationStrategy.{CounterKey, ValueData}
import com.evolutiongaming.util.ActorSpec
import com.typesafe.config.ConfigValueFactory
import org.mockito.Mockito._
import org.mockito.{Matchers => MM}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.collection.immutable
import scala.compat.Platform
import scala.concurrent.duration._

class AdaptiveAllocationStrategyDistributedDataSpec extends FlatSpec
  with ActorSpec
  with Matchers
  with MockitoSugar
  with OptionValues
  with ScalaFutures
  with Eventually
  with PatienceConfiguration {

  override implicit val patienceConfig = PatienceConfig(5.seconds, 500.millis)

  override def config = super.config withValue
    ("akka.actor.provider", ConfigValueFactory fromAnyRef "akka.cluster.ClusterActorRefProvider")

  import AdaptiveAllocationStrategyDistributedDataProxy._

  "AdaptiveAllocationStrategy" should "correctly increment and clear a counter" in new Scope {

    val extractShardId = strategy wrapExtractShardId ExtractShardId.identity

    // increment
    extractShardId(ShardedMsg(entityId, new Serializable {})) shouldBe entityId

    expectMsgPF() {
      case Subscribe(key, _) if key.id == expectedCounterKey.toString =>
    }

    expectMsgPF() {
      case Update(key, WriteLocal, _) if key.id == expectedCounterKey.toString =>
    }

    expectMsgPF() {
      case Update(EntityToNodeCountersKey, WriteLocal, _) =>
    }

    val entityCounters = AdaptiveAllocationStrategy.entityToNodeCounters get expectedEntityKey
    entityCounters.value should (contain (expectedCounterKey) and have size 1)

    val valueData = AdaptiveAllocationStrategy.counters get expectedCounterKey
    valueData.value.value shouldBe 1
    val timestamp = valueData.value.cleared

    // clear
    strategy.clear(TypeName, entityId)

    expectMsgPF() {
      case Update(key, WriteLocal, _) if key.id == expectedCounterKey.toString =>
    }

    val clearedData = AdaptiveAllocationStrategy.counters get expectedCounterKey
    clearedData.value.value shouldBe 0
    clearedData.value.cleared should be > timestamp
  }

  it should "correctly process counter updates from distributed data" in new Scope {

    val counter = PNCounter.empty + 2

    proxy ! Changed(PNCounterKey(expectedCounterKey.toString))(counter)

    val timestamp = eventually {
      val valueData = AdaptiveAllocationStrategy.counters get expectedCounterKey
      valueData.value.value shouldBe 2
      valueData.value.cleared
    }

    proxy ! Changed(PNCounterKey(expectedCounterKey.toString))(counter + 4)

    eventually {
      val updatedValueData = AdaptiveAllocationStrategy.counters get expectedCounterKey
      updatedValueData.value.value shouldBe 6
      updatedValueData.value.cleared shouldBe timestamp
    }
  }

  it should "correctly process map updates from distributed data" in new Scope {

    AdaptiveAllocationStrategy.entityToNodeCounters should have size 0

    override val map = ORMultiMap.empty[String, String] + (expectedEntityKey.toString -> Set(expectedCounterKey.toString))

    proxy ! Changed(EntityToNodeCountersKey)(map)

    eventually {
      val entityCounters = AdaptiveAllocationStrategy.entityToNodeCounters get expectedEntityKey
      entityCounters.value should (contain (expectedCounterKey) and have size 1)
    }
  }

  it should "allocate a shard on the requester node if the counters is empty" in new Scope {

    val requester = TestProbe().testActor

    strategy.allocateShard(
      requester = requester,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe requester
  }

  it should "allocate a shard on a node (local) with the biggest counter value" in new Scope {

    proxy ! Changed(EntityToNodeCountersKey)(map)

    eventually {
      AdaptiveAllocationStrategy.entityToNodeCounters should have size 4
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(100500, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(100, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(500, Platform.currentTime))

    strategy.allocateShard(
      requester = TestProbe().testActor,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe localAddressRef
  }

  it should "allocate a shard on a node (remote) with the biggest counter value" in new Scope {

    proxy ! Changed(EntityToNodeCountersKey)(map)

    eventually {
      AdaptiveAllocationStrategy.entityToNodeCounters should have size 4
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(100, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(500, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(100500, Platform.currentTime))

    strategy.allocateShard(
      requester = TestProbe().testActor,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe anotherAddressRef2
  }

  it should "allocate a shard on a node (local) with the biggest counter value (respect cummulative home node counter)" in new Scope {

    proxy ! Changed(EntityToNodeCountersKey)(map)

    eventually {
      AdaptiveAllocationStrategy.entityToNodeCounters should have size 4
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(500, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(100, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(700, Platform.currentTime))

    strategy.allocateShard(
      requester = TestProbe().testActor,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe localAddressRef
  }

  it should "allocate a shard on a node (remote) with the biggest counter value (respect cummulative home node counter)" in new Scope {

    proxy ! Changed(EntityToNodeCountersKey)(map)

    eventually {
      AdaptiveAllocationStrategy.entityToNodeCounters should have size 4
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(100, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(700, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(500, Platform.currentTime))

    strategy.allocateShard(
      requester = TestProbe().testActor,
      shardId = entityId1,
      currentShardAllocations = noShard1ShardAllocations).futureValue shouldBe anotherAddressRef2
  }

  it should "rebalance shards if the difference between non-home and home counters is bigger than rebalanceThreshold" in new Scope {

    proxy ! Changed(EntityToNodeCountersKey)(map)

    (1 to 12) foreach { _ =>
      expectMsgPF() {
        case Subscribe(key: PNCounterKey, _) =>
      }
    }

    eventually {
      AdaptiveAllocationStrategy.entityToNodeCounters should have size 4
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome2 -> ValueData(counterHome2, Platform.currentTime - cleanupPeriodMillis))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome21 -> ValueData(counterNonHome21, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome22 -> ValueData(counterNonHome22, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3 -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31 -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32 -> ValueData(counterNonHome32, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome4 -> ValueData(counterHome4, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome41 -> ValueData(counterNonHome41, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome42 -> ValueData(counterNonHome42, Platform.currentTime - cleanupPeriodMillis))

    // should rebalance 1,4 and clear all
    val result1 = strategy.rebalance(
      shardAllocations,
      rebalanceInProgress = Set.empty[ShardId]).futureValue

    result1 shouldBe Set(entityId1, entityId4)

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome2 -> ValueData(counterHome2, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome21 -> ValueData(counterNonHome21, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome22 -> ValueData(counterNonHome22, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3 -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31 -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32 -> ValueData(counterNonHome32, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome4 -> ValueData(counterHome4, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome41 -> ValueData(counterNonHome41, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome42 -> ValueData(counterNonHome42, Platform.currentTime))

    // 1 is in progress - should rebalance 4
    val result2 = strategy.rebalance(
      shardAllocations,
      rebalanceInProgress = Set[ShardId](entityId1)).futureValue

    result2 shouldBe Set(entityId4)

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome2 -> ValueData(counterHome2, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome21 -> ValueData(counterNonHome21, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome22 -> ValueData(counterNonHome22, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3 -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31 -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32 -> ValueData(counterNonHome32, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome4 -> ValueData(counterHome4, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome41 -> ValueData(counterNonHome41, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome42 -> ValueData(counterNonHome42, Platform.currentTime))

    // 1,4 is in progress - should not rebalance
    val result3 = strategy.rebalance(
      shardAllocations,
      rebalanceInProgress = Set[ShardId](entityId4, entityId1)).futureValue

    result3 shouldBe Set()

    AdaptiveAllocationStrategy.counters += (counterKeyHome1 -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11 -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12 -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome2 -> ValueData(counterHome2, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome21 -> ValueData(counterNonHome21, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome22 -> ValueData(counterNonHome22, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3 -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31 -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32 -> ValueData(counterNonHome32, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome4 -> ValueData(counterHome4, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome41 -> ValueData(counterNonHome41, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome42 -> ValueData(counterNonHome42, Platform.currentTime))

    // limit rebalance to 1 - should rebalance 4
    val strategy1 = new AdaptiveAllocationStrategy(
      typeName = TypeName,
      rebalanceThresholdPercent = RebalanceThresholdPercent,
      cleanupPeriod = CleanupPeriod,
      metricRegistry = metricRegistry,
      countControl = CountControl.Increment,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = 1,
      nodesToDeallocate = () => Set.empty)

    val result5 = strategy1.rebalance(
      shardAllocations,
      rebalanceInProgress = Set.empty[ShardId]).futureValue

    result5 shouldBe Set(entityId4)
  }

  abstract class Scope extends ActorScope with DefaultTimeout {

    val MaxSimultaneousRebalance: Int = 10
    val RebalanceThresholdPercent: Int = 30
    val CleanupPeriod: FiniteDuration = 10.minutes

    AdaptiveAllocationStrategy.counters.clear()
    AdaptiveAllocationStrategy.entityToNodeCounters = Map.empty

    implicit val node = Cluster(system)

    implicit val ec = system.dispatcher

    val TypeName = "typeName"

    val selfAddress = node.selfAddress.toString

    def entityKey(entityId: String) = AdaptiveAllocationStrategy.EntityKey(TypeName, entityId)
    def counterKey(entityId: String, address: String = selfAddress) =
      AdaptiveAllocationStrategy.CounterKey(entityKey(entityId), address)

    val entityId = "entityId"
    val expectedEntityKey = entityKey(entityId)
    val expectedCounterKey = counterKey(entityId)

    val cleanupPeriodMillis = CleanupPeriod.toMillis

    def mockedAddressRef(addr: Address): ActorRef = {
      val rootPath = RootActorPath(addr)
      val path = new ChildActorPath(rootPath, "test")
      new TestDummyActorRef(path)
    }

    def mockedHostRef(host: String): ActorRef = {
      val addr = Address(
        protocol = "http",
        system = "System",
        host = host,
        port = 2552)
      mockedAddressRef(addr)
    }

    val localAddressRef = mockedAddressRef(node.selfAddress)
    val anotherAddressRef1 = mockedHostRef("anotherAddress1")
    val anotherAddressRef2 = mockedHostRef("anotherAddress2")

    val anotherAddress1 = anotherAddressRef1.path.address.toString
    val anotherAddress2 = anotherAddressRef2.path.address.toString

    // rebalance
    val entityId1 = "1"
    val entityKey1 = entityKey(entityId1)
    val counterKeyHome1 = counterKey(entityId1)
    val counterKeyNonHome11 = counterKey(entityId1, anotherAddress1)
    val counterKeyNonHome12 = counterKey(entityId1, anotherAddress2)
    val counterHome1 = 700
    val counterNonHome11 = 100
    val counterNonHome12 = 500

    // clear
    val entityId2 = "2"
    val entityKey2 = entityKey(entityId2)
    val counterKeyHome2 = counterKey(entityId2, anotherAddress1)
    val counterKeyNonHome21 = counterKey(entityId2)
    val counterKeyNonHome22 = counterKey(entityId2, anotherAddress2)
    val counterHome2 = 900
    val counterNonHome21 = 100
    val counterNonHome22 = 200

    // no action (threshold < 30%)
    val entityId3 = "3"
    val entityKey3 = entityKey(entityId3)
    val counterKeyHome3 = counterKey(entityId3)
    val counterKeyNonHome31 = counterKey(entityId3, anotherAddress1)
    val counterKeyNonHome32 = counterKey(entityId3, anotherAddress2)
    val counterHome3 = 700
    val counterNonHome31 = 200
    val counterNonHome32 = 300

    // rebalance and clear
    val entityId4 = "4"
    val entityKey4 = entityKey(entityId4)
    val counterKeyHome4 = counterKey(entityId4, anotherAddress2)
    val counterKeyNonHome41 = counterKey(entityId4, anotherAddress1)
    val counterKeyNonHome42 = counterKey(entityId4)
    val counterHome4 = 700
    val counterNonHome41 = 100
    val counterNonHome42 = 500

    def mapCounterKeys(set: Set[CounterKey]): Set[PNCounterKey] = set map { key => PNCounterKey(key.toString) }

    val map = ORMultiMap.empty[String, String] +
      (entityKey1.toString -> (Set(counterKeyNonHome11, counterKeyHome1, counterKeyNonHome12) map (_.toString))) +
      (entityKey2.toString -> (Set(counterKeyNonHome21, counterKeyHome2, counterKeyNonHome22) map (_.toString))) +
      (entityKey3.toString -> (Set(counterKeyHome3, counterKeyNonHome31, counterKeyNonHome32) map (_.toString))) +
      (entityKey4.toString -> (Set(counterKeyNonHome41, counterKeyNonHome42, counterKeyHome4) map (_.toString)))

    val shardAllocations = Map[ActorRef, immutable.IndexedSeq[ShardId]](
      anotherAddressRef1 -> immutable.IndexedSeq[ShardId](entityId2),
      anotherAddressRef2 -> immutable.IndexedSeq[ShardId](entityId4),
      localAddressRef -> immutable.IndexedSeq[ShardId](entityId3, entityId1))

    val noShard1ShardAllocations = Map[ActorRef, immutable.IndexedSeq[ShardId]](
      anotherAddressRef1 -> immutable.IndexedSeq[ShardId](entityId2),
      anotherAddressRef2 -> immutable.IndexedSeq[ShardId](entityId4),
      localAddressRef -> immutable.IndexedSeq[ShardId](entityId3))

    val metricRegistry = mock[MetricRegistry]
    when(metricRegistry.meter(MM.anyString())) thenReturn mock[Meter]

    val proxy = system actorOf Props(new TestProxy)

    val fallbackStrategy = new RequesterAllocationStrategy(
      maxSimultaneousRebalance = MaxSimultaneousRebalance,
      nodesToDeallocate = () => Set.empty)

    val strategy = new AdaptiveAllocationStrategy(
      typeName = TypeName,
      rebalanceThresholdPercent = RebalanceThresholdPercent,
      cleanupPeriod = CleanupPeriod,
      metricRegistry = metricRegistry,
      countControl = CountControl.Increment,
      fallbackStrategy = fallbackStrategy,
      proxy = proxy,
      maxSimultaneousRebalance = MaxSimultaneousRebalance,
      nodesToDeallocate = () => Set.empty)

    expectMsgPF() {
      case Subscribe(EntityToNodeCountersKey, _) =>
    }

    class TestProxy extends AdaptiveAllocationStrategyDistributedDataProxy {
      override lazy val replicator = testActor
    }
  }
}
