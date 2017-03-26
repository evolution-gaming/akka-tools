package com.evolutiongaming.util

import org.scalatest.concurrent.{ScalaFutures, TimeLimits, Timeouts}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NoStackTrace

class FutureSequentialForKeySpec extends WordSpec with ActorSpec with Matchers with ScalaFutures with TimeLimits {
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
          case key1   => true
        } shouldEqual "key1-1"

        fishForMessage() {
          case "key2" => false
          case key1   => true
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

    "be fast" in new Scope {
      import system.dispatcher

      val keys = 1 to 100

      val futures = failAfter(5.seconds) {
        for {key <- keys.par; _ <- 1 to 100} futureSequentialForKey(key) {()}
        for {key <- keys} yield futureSequentialForKey(key) {key}
      }

      failAfter(timeout.duration) {
        for {key <- keys} yield futureSequentialForKey(key) {key}
        val result = Await.result(Future.sequence(futures), timeout.duration * 2)
        result.toSet shouldEqual keys.toSet
      }
    }
  }


  trait Scope extends ActorScope {
    def futureSequentialForKey = FutureSequentialForKey(system)
  }
}
