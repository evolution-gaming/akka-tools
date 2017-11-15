package com.evolutiongaming.serialization

import java.time.Instant

import play.api.libs.json.{Format, Json}


case class TestCaseClassA(
  field1: String,
  field2: BigDecimal = 0,
  field3: Int = 0)

object TestCaseClassA {
  lazy val Test: TestCaseClassA = TestCaseClassA("test")

  implicit val JsonFormat: Format[TestCaseClassA] = Json.format[TestCaseClassA]
}

sealed trait TestTraitB extends Serializable {
  def timestamp: Instant

  def eventName: String
}

sealed trait TraitC extends TestTraitB

sealed trait TraitD extends TraitC {
  def eventName: String = getClass.getSimpleName
}

sealed trait TraitE extends TraitD

case class TestCaseClassB(userId: String, timestamp: Instant, freeText: Option[String] = None) extends TraitE
