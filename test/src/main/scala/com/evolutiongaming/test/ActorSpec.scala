package com.evolutiongaming.test

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Suite}
import com.typesafe.config.Config

trait ActorSpec extends BeforeAndAfterAll { this: Suite =>

  implicit lazy val system: ActorSystem = ActorSystem(getClass.getSimpleName, config)

  def config: Config = TestConfig()

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  abstract class ActorScope extends TestKit(system) with ImplicitSender with DefaultTimeout
}
