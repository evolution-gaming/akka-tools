package com.evolutiongaming.cluster

import akka.TestDummyActorRef
import akka.actor.{ActorRef, Address, ChildActorPath, RootActorPath}
import akka.testkit.DefaultTimeout
import com.evolutiongaming.util.ActorSpec
import org.scalatest.{FlatSpec, Matchers, OptionValues}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.duration._

trait AllocationStrategySpec extends FlatSpec
  with ActorSpec
  with Matchers
  with MockitoSugar
  with OptionValues
  with ScalaFutures
  with Eventually
  with PatienceConfiguration {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 500.millis)

  trait AllocationStrategyScope extends ActorScope with DefaultTimeout {

    implicit val ec = system.dispatcher

    def mockedAddressRef(addr: Address): ActorRef = {
      val rootPath = RootActorPath(addr)
      val path = new ChildActorPath(rootPath, "test")
      new TestDummyActorRef(path)
    }

    def mockedHostRef(host: String): ActorRef =
      mockedAddressRef(testAddress(host))

    def testAddress(host: String): Address = Address(
      protocol = "http",
      system = "System",
      host = host,
      port = 2552)
  }
}