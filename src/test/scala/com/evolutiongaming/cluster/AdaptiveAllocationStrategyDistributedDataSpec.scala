package com.evolutiongaming.cluster

import akka.TestDummyActorRef
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.{Changed, Subscribe, Update, WriteLocal}
import akka.cluster.ddata.{ORMultiMap, PNCounter, PNCounterKey}
import akka.cluster.sharding.ShardRegion.ShardId
import akka.testkit.{DefaultTimeout, TestProbe}
import com.codahale.metrics.{Meter, MetricRegistry}
import com.evolutiongaming.cluster.AdaptiveAllocationStrategy.ValueData
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

  override def config = super.config withValue
    ("akka.actor.provider", ConfigValueFactory fromAnyRef "akka.cluster.ClusterActorRefProvider")

  import AdaptiveAllocationStrategyDistributedDataProxy._


  "AdaptiveAllocationStrategy" should "correctly increment and clear a counter" in new Scope {

    val extractShardId = strategy.extractShardId(0)

    // increment
    extractShardId(ClusterMsg(entityId, new Serializable {})) shouldBe entityId

    expectMsgPF() {
      case Subscribe(EntityToNodeCountersKey, _) =>
    }

    expectMsgPF() {
      case Subscribe(`expectedCounterKey`, _) =>
    }

    expectMsgPF() {
      case Update(`expectedCounterKey`, WriteLocal, _) =>
    }

    expectMsgPF() {
      case Update(EntityToNodeCountersKey, WriteLocal, _) =>
    }

    val entityCounters = AdaptiveAllocationStrategy.entityToNodeCounters get expectedEntityKeyStr
    entityCounters.value should (contain (expectedCounterKey._id) and have size 1)

    val valueData = AdaptiveAllocationStrategy.counters get expectedCounterKey._id
    valueData.value.value shouldBe 1
    val timestamp = valueData.value.cleared

    // clear
    AdaptiveAllocationStrategy.clear(typeName, entityId)

    expectMsgPF() {
      case Update(`expectedCounterKey`, WriteLocal, _) =>
    }

    val clearedData = AdaptiveAllocationStrategy.counters get expectedCounterKey._id
    clearedData.value.value shouldBe 0
    clearedData.value.cleared should be > timestamp
  }

  it should "correctly process counter updates from distributed data" in new Scope {

    val proxy = AdaptiveAllocationStrategy.proxy.value

    val counter = PNCounter.empty + 2

    proxy ! Changed(expectedCounterKey)(counter)

    val timestamp = eventually {
      val valueData = AdaptiveAllocationStrategy.counters get expectedCounterKey._id
      valueData.value.value shouldBe 2
      valueData.value.cleared
    }

    proxy ! Changed(expectedCounterKey)(counter + 4)

    eventually {
      val updatedValueData = AdaptiveAllocationStrategy.counters get expectedCounterKey._id
      updatedValueData.value.value shouldBe 6
      updatedValueData.value.cleared shouldBe timestamp
    }
  }

  it should "correctly process map updates from distributed data" in new Scope {

    val proxy = AdaptiveAllocationStrategy.proxy.value

    AdaptiveAllocationStrategy.entityToNodeCounters should have size 0

    val map = ORMultiMap.empty[String] + (expectedEntityKeyStr -> Set(expectedCounterKey._id))

    proxy ! Changed(EntityToNodeCountersKey)(map)

    eventually {
      val entityCounters = AdaptiveAllocationStrategy.entityToNodeCounters get expectedEntityKeyStr
      entityCounters.value should (contain (expectedCounterKey._id) and have size 1)
    }
  }

  it should "allocate shards on the requester node" in new Scope {
    
    val requester = TestProbe().testActor // mock[ActorRef]

    strategy.allocateShard(
      requester = requester,
      shardId = "",
      currentShardAllocations = Map.empty).futureValue shouldBe requester
  }

  it should "rebalance shards if the difference between non-home and home counters is bigger than rebalanceThreshold" in new Scope {

    expectMsgPF() {
      case Subscribe(EntityToNodeCountersKey, _) =>
    }

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

    val anotherAddressRef1 = mockedHostRef("anotherAddress1")
    val anotherAddressRef2 = mockedHostRef("anotherAddress2")

    val anotherAddress1 = anotherAddressRef1.path.address.toString
    val anotherAddress2 = anotherAddressRef2.path.address.toString

    // rebalance
    val entityId1 = "1"
    val entityKeyStr1 = entityKey(entityId1)
    val counterKeyHome1 = PNCounterKey(counterKey(entityId1))
    val counterKeyNonHome11 = PNCounterKey(counterKey(entityId1, anotherAddress1))
    val counterKeyNonHome12 = PNCounterKey(counterKey(entityId1, anotherAddress2))
    val counterHome1 = 100
    val counterNonHome11 = 500
    val counterNonHome12 = 100500

    // clear
    val entityId2 = "2"
    val entityKeyStr2 = entityKey(entityId2)
    val counterKeyHome2 = PNCounterKey(counterKey(entityId2, anotherAddress1))
    val counterKeyNonHome21 = PNCounterKey(counterKey(entityId2))
    val counterKeyNonHome22 = PNCounterKey(counterKey(entityId2, anotherAddress2))
    val counterHome2 = 100500
    val counterNonHome21 = 100
    val counterNonHome22 = 500

    // rebalance
    val entityId3 = "3"
    val entityKeyStr3 = entityKey(entityId3)
    val counterKeyHome3 = PNCounterKey(counterKey(entityId3))
    val counterKeyNonHome31 = PNCounterKey(counterKey(entityId3, anotherAddress1))
    val counterKeyNonHome32 = PNCounterKey(counterKey(entityId3, anotherAddress2))
    val counterHome3 = 500
    val counterNonHome31 = 100
    val counterNonHome32 = 100500

    // rebalance and clear
    val entityId4 = "4"
    val entityKeyStr4 = entityKey(entityId4)
    val counterKeyHome4 = PNCounterKey(counterKey(entityId4, anotherAddress2))
    val counterKeyNonHome41 = PNCounterKey(counterKey(entityId4, anotherAddress1))
    val counterKeyNonHome42 = PNCounterKey(counterKey(entityId4))
    val counterHome4 = 100
    val counterNonHome41 = 500
    val counterNonHome42 = 100500

    val map = ORMultiMap.empty[String] +
      (entityKeyStr1 -> Set(counterKeyNonHome11._id, counterKeyHome1._id, counterKeyNonHome12._id)) +
      (entityKeyStr2 -> Set(counterKeyNonHome21._id, counterKeyHome2._id, counterKeyNonHome22._id)) +
      (entityKeyStr3 -> Set(counterKeyHome3._id, counterKeyNonHome31._id, counterKeyNonHome32._id)) +
      (entityKeyStr4 -> Set(counterKeyNonHome41._id, counterKeyNonHome42._id, counterKeyHome4._id))

    val proxy = AdaptiveAllocationStrategy.proxy.value

    proxy ! Changed(EntityToNodeCountersKey)(map)

    (1 to 12) foreach { _ =>
      expectMsgPF() {
        case Subscribe(key: PNCounterKey, _) =>
      }
    }

    eventually {
      AdaptiveAllocationStrategy.entityToNodeCounters should have size 4
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1._id -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11._id -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12._id -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome2._id -> ValueData(counterHome2, Platform.currentTime - cleanupPeriodMillis))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome21._id -> ValueData(counterNonHome21, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome22._id -> ValueData(counterNonHome22, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3._id -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31._id -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32._id -> ValueData(counterNonHome32, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome4._id -> ValueData(counterHome4, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome41._id -> ValueData(counterNonHome41, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome42._id -> ValueData(counterNonHome42, Platform.currentTime - cleanupPeriodMillis))

    val currentShardAllocations = Map[ActorRef, immutable.IndexedSeq[ShardId]](
      anotherAddressRef1 -> immutable.IndexedSeq[ShardId](entityId2),
      anotherAddressRef2 -> immutable.IndexedSeq[ShardId](entityId4),
      mockedAddressRef(node.selfAddress) -> immutable.IndexedSeq[ShardId](entityId3, entityId1))

    // should rebalance 1,3,4 and clear all
    val result1 = strategy.rebalance(
      currentShardAllocations,
      rebalanceInProgress = Set.empty[ShardId]).futureValue

    result1 shouldBe Set(entityId1, entityId3, entityId4)

    // clear
    (1 to 12) foreach { _ =>
      expectMsgPF() {
        case Update(key: PNCounterKey, WriteLocal, _) =>
      }
    }

    AdaptiveAllocationStrategy.counters += (counterKeyHome1._id -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11._id -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12._id -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3._id -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31._id -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32._id -> ValueData(counterNonHome32, Platform.currentTime))

    // 1 is in progress - should rebalance 3
    val result2 = strategy.rebalance(
      currentShardAllocations,
      rebalanceInProgress = Set[ShardId](entityId1)).futureValue

    result2 shouldBe Set(entityId3)

    AdaptiveAllocationStrategy.counters += (counterKeyHome1._id -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11._id -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12._id -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3._id -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31._id -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32._id -> ValueData(counterNonHome32, Platform.currentTime))

    // 1,3 is in progress - should not rebalance
    val result3 = strategy.rebalance(
      currentShardAllocations,
      rebalanceInProgress = Set[ShardId](entityId3, entityId1)).futureValue

    result3 shouldBe Set()

    AdaptiveAllocationStrategy.counters += (counterKeyHome1._id -> ValueData(counterHome1, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome11._id -> ValueData(counterNonHome11, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome12._id -> ValueData(counterNonHome12, Platform.currentTime))

    AdaptiveAllocationStrategy.counters += (counterKeyHome3._id -> ValueData(counterHome3, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome31._id -> ValueData(counterNonHome31, Platform.currentTime))
    AdaptiveAllocationStrategy.counters += (counterKeyNonHome32._id -> ValueData(counterNonHome32, Platform.currentTime))

    // limit rebalance to 1 - should rebalance 3
    val strategy1 = AdaptiveAllocationStrategy(
      typeName = typeName,
      maxSimultaneousRebalance = 1,
      rebalanceThreshold = RebalanceThreshold,
      cleanupPeriod = CleanupPeriod,
      metricRegistry = metricRegistry)(proxyProps = Props(new TestProxy))

    val result5 = strategy1.rebalance(
      currentShardAllocations,
      rebalanceInProgress = Set.empty[ShardId]).futureValue

    result5 shouldBe Set(entityId3)
  }

  abstract class Scope extends ActorScope with DefaultTimeout {

    val MaxSimultaneousRebalance: Int = 10
    val RebalanceThreshold: Int = 1000
    val CleanupPeriod: FiniteDuration = 10.minutes

    AdaptiveAllocationStrategy.counters.clear()
    AdaptiveAllocationStrategy.entityToNodeCounters = Map.empty
    AdaptiveAllocationStrategy.proxy = None

    implicit val node = Cluster(system)

    val typeName = "typeName"

    val selfAddress = node.selfAddress.toString

    def entityKey(entityId: String) = AdaptiveAllocationStrategy.genEntityKey(typeName, entityId)
    def counterKey(entityId: String, address: String = selfAddress) =
      AdaptiveAllocationStrategy.genCounterKey(entityKey(entityId), address)

    val entityId = "entityId"
    val expectedEntityKeyStr = entityKey(entityId)
    val expectedCounterKey = PNCounterKey(counterKey(entityId))

    val metricRegistry = mock[MetricRegistry]
    when(metricRegistry.meter(MM.anyString())) thenReturn mock[Meter]

    val strategy = AdaptiveAllocationStrategy(
      typeName = typeName,
      maxSimultaneousRebalance = MaxSimultaneousRebalance,
      rebalanceThreshold = RebalanceThreshold,
      cleanupPeriod = CleanupPeriod,
      metricRegistry = metricRegistry)(proxyProps = Props(new TestProxy))

    class TestProxy extends AdaptiveAllocationStrategyDistributedDataProxy {
      override lazy val replicator = testActor
    }
  }
}
