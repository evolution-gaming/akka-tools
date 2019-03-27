package com.evolutiongaming.test

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContext

trait ActorSpec extends BeforeAndAfterAll { this: Suite =>

  implicit lazy val system: ActorSystem = ActorSystem(
    name = getClass.getSimpleName,
    config = Some(config),
    defaultExecutionContext = defaultExecutionContext)

  def defaultExecutionContext: Option[ExecutionContext] = Some(ExecutionContext.global)

  def config: Config = TestConfig()

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  abstract class ActorScope extends TestKit(system) with ImplicitSender with DefaultTimeout
}
