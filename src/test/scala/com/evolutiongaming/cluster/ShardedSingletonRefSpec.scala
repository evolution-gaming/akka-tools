package com.evolutiongaming.cluster

import com.evolutiongaming.util.ActorSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.FiniteDuration

class ShardedSingletonRefSpec extends WordSpec with ActorSpec with ScalaFutures with Matchers {

  "Impl" should {

    "tell" in new Scope {
      ref.tell(In, testActor)
      expectMsg(shardedMsg)
      lastSender shouldEqual testActor
    }

    "tellUnsafe" in new Scope {
      ref.tellUnsafe(Unsafe, testActor)
      expectMsg(shardedUnsafe)
      lastSender shouldEqual testActor
    }

    "ask" in new Scope {
      val future = ref.ask(In)
      expectMsg(shardedMsg)
      lastSender ! Out
      whenReady(future) { _ shouldEqual Out }
    }

    "askUnsafe" in new Scope {
      val future = ref.askUnsafe(Unsafe)
      expectMsg(shardedUnsafe)
      lastSender ! Out
      whenReady(future) { _ shouldEqual Out }
    }
  }

  private trait Scope extends ActorScope {
    val shardId = "shardId"
    val shardedMsg = ShardedMsg(shardId, In)
    val shardedUnsafe = ShardedMsg(shardId, Unsafe)
    val ref = ShardedSingletonRef(shardId, ShardedRef[String, In.type, Out.type](testActor))

    implicit def duration: FiniteDuration = timeout.duration

    case object In
    case object Out
    case object Unsafe
  }
}
