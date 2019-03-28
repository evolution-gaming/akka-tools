package com.evolutiongaming.test

import java.util.concurrent.Executors

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

  def defaultExecutionContext: Option[ExecutionContext] = {
    val parallelism = 4 + Runtime.getRuntime.availableProcessors() * 2
    val es = Executors.newWorkStealingPool(parallelism)
    val ec = ExecutionContext.fromExecutorService(es)
    Some(ec)
  }

  def config: Config = TestConfig()

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  abstract class ActorScope extends TestKit(system) with ImplicitSender with DefaultTimeout
}
