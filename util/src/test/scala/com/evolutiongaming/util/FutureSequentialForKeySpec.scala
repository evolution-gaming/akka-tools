package com.evolutiongaming.util

import com.evolutiongaming.test.ActorSpec
import org.scalatest.concurrent.{ScalaFutures, TimeLimits}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.control.NoStackTrace

class FutureSequentialForKeySpec extends AnyWordSpec with ActorSpec with Matchers with ScalaFutures with TimeLimits {
  "FutureSequentialForKey" should {
    "perform tasks for same key sequentially in same order they were sent" in new Scope {
      for {_ <- 0 to 10} {
        futureSequentialForKey("key1") {
          Thread.sleep(100)
          testActor ! "key1-1"
        }

        futureSequentialForKey("key2") {
          testActor ! "key2"
        }

        futureSequentialForKey("key1") {
          testActor ! "key1-2"
        }

        fishForMessage() {
          case "key2" => false
          case _   => true
        } shouldEqual "key1-1"

        fishForMessage() {
          case "key2" => false
          case _   => true
        } shouldEqual "key1-2"
      }
    }

    "make sure failure of prev func have no impact on the next one" in new Scope {
      futureSequentialForKey("key") {
        throw new RuntimeException with NoStackTrace
      }

      futureSequentialForKey("key") {testActor ! 1}
      expectMsg(1)

      val future = futureSequentialForKey("key") {"2"}
      whenReady(future) {_ shouldEqual "2"}
    }
  }


  trait Scope extends ActorScope {
    def futureSequentialForKey = FutureSequentialForKey(system)
  }
}
