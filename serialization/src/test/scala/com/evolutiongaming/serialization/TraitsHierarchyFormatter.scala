package com.evolutiongaming.serialization

import com.evolutiongaming.serialization.{TestTraitB => E}
import play.api.libs.json._


object TraitsHierarchyFormatter extends Format[E] {

  implicit val RealityCheckedFormat: OFormat[TestCaseClassB] = Json.format[TestCaseClassB]
  
  def writes(x: E): JsObject = {
    def json[T <: E](t: String, e: T)(implicit write: Writes[T]) = {
      Json.obj("type" -> t, "value" -> (Json toJson e))
    }

    x match {
      case x: TestCaseClassB => json("TestCaseClassB", x)
    }
  }

  def reads(json: JsValue): JsResult[E] = {
    def value(t: String): JsResult[E] = {
      def value = json \ "value"

      t match {
        case "TestCaseClassB" => value.validate[TestCaseClassB]
        case _ => JsError(s"Unexpected type: $t, json: $json")
      }
    }

    for {
      t <- (json \ "type").validate[String]
      v <- value(t)
    } yield v
  }
}
