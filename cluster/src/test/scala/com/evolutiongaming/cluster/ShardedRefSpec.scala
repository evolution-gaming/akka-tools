package com.evolutiongaming.cluster

import com.evolutiongaming.test.ActorSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Format

class ShardedRefSpec extends WordSpec with ActorSpec with ScalaFutures with Matchers {

  "Impl" should {

    "tell" in new ImplScope {
      ref.tell(shardId, In, testActor)
      expectMsg(shardedMsg)
      lastSender shouldEqual testActor
    }

    "tellUnsafe" in new ImplScope {
      ref.tellUnsafe(shardId, Unsafe, testActor)
      expectMsg(shardedUnsafe)
      lastSender shouldEqual testActor
    }

    "ask" in new ImplScope {
      val future = ref.ask(shardId, In)
      expectMsg(shardedMsg)
      lastSender ! Out
      whenReady(future) { _ shouldEqual Out }
    }

    "askUnsafe" in new ImplScope {
      val future = ref.askUnsafe(shardId, Unsafe)
      expectMsg(shardedUnsafe)
      lastSender ! Out
      whenReady(future) { _ shouldEqual Out }
    }
  }

  "Proxy" should {

    "tell" in new ProxyScope {
      ref.tell(shardId, In, testActor)
      expectMsg(In)
      lastSender shouldEqual testActor
    }

    "tellUnsafe" in new ProxyScope {
      ref.tellUnsafe(shardId, Unsafe, testActor)
      expectMsg(Unsafe)
      lastSender shouldEqual testActor
    }

    "ask" in new ProxyScope {
      val future = ref.ask(shardId, In)
      expectMsg(In)
      lastSender ! Out
      whenReady(future) { _ shouldEqual Out }
    }

    "askUnsafe" in new ProxyScope {
      val future = ref.askUnsafe(shardId, Unsafe)
      expectMsg(Unsafe)
      lastSender ! Out
      whenReady(future) { _ shouldEqual Out }
    }
  }

  private trait Scope extends ActorScope {
    val shardId = "shardId"

    implicit def duration = timeout.duration

    sealed trait Msg

    case object In extends Msg
    case object Out extends Msg
    case object Unsafe extends Msg

    implicit val MsgFormat: Format[Msg] = null
  }

  private trait ImplScope extends Scope {
    val shardedMsg = ShardedMsg(shardId, In)
    val shardedUnsafe = ShardedMsg(shardId, Unsafe)
    val ref = ShardedRef[String, In.type, Out.type](testActor)
  }

  private trait ProxyScope extends Scope {
    val ref = ShardedRef.Proxy[String, In.type, Out.type](testActor)
  }
}
