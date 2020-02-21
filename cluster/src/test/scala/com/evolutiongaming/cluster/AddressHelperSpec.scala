package com.evolutiongaming.cluster

import akka.actor.Address
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AddressHelperSpec extends AnyFunSuite with Matchers {
  val global = Address("akka.tcp", "coreservices", "127.0.0.1", 9196)
  val local = Address("akka", "coreservices")
  val addressHelper = new AddressHelper(global)

  test("toLocal") {
    addressHelper.toLocal(global) shouldEqual local
    addressHelper.toLocal(local) shouldEqual local
  }

  test("toGlobal") {
    addressHelper.toGlobal(global) shouldEqual global
    addressHelper.toGlobal(local) shouldEqual global
  }
}
