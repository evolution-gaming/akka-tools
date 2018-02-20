package com.evolutiongaming.serialization

import java.nio.charset.Charset

import akka.persistence.serialization.Snapshot
import com.evolutiongaming.util.ToJsonStr
import com.github.t3hnar.scalax.RichAny
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._

import scala.util._

class SnapshotJsonSerializer(
  bindings: Bindings,
  fallbackSerializer: akka.serialization.Serializer,
  binarySerializer: akka.serialization.Serializer) extends LazyLogging {

  import SnapshotJsonSerializer._

  def toBinary(snapshot: Snapshot): Array[Byte] = {
    val result = for {
      anyRef <- snapshot.data.asInstanceOfOpt[AnyRef]
      (name, payload) <- bindings.toJson(anyRef)
    } yield {
      val snapshotWithJson = SnapshotWithJson(name, anyRef.getClass.getName, payload)
      val str = ToJsonStr(snapshotWithJson)
      str getBytes UTF8
    }

    result getOrElse fallbackSerializer.toBinary(snapshot)
  }

  def fromBinary(bytes: Array[Byte]): Snapshot =
    Try(Json.parse(bytes)) match {
      case Success(json) =>
        val snapshotWithJson = json.as[SnapshotWithJson]
        val payload = bindings.fromJsonOrError(snapshotWithJson.`type`, snapshotWithJson.payload)
        Snapshot(payload)

      case Failure(t) =>
        logger.error(s"Error parsing JSON snapshot ${ new String(bytes) }", t)
        val anyRef = fallbackSerializer.fromBinary(bytes, classOf[Snapshot])
        anyRef.asInstanceOf[Snapshot]
    }
}

object SnapshotJsonSerializer {
  val UTF8: Charset = Charset.forName("UTF-8")


  case class SnapshotWithJson(`type`: String, manifest: String, payload: JsObject)

  object SnapshotWithJson {
    implicit val SnapshotWithJsonFormat: Format[SnapshotWithJson] = Json.format[SnapshotWithJson]
  }
}
