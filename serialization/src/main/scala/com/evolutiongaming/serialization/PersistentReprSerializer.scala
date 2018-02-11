package com.evolutiongaming.serialization

import java.nio.charset.Charset

import akka.persistence.PersistentRepr
import com.github.t3hnar.scalax._
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._

import scala.util._

class PersistentReprSerializer(bindings: Bindings, fallbackSerializer: akka.serialization.Serializer) extends LazyLogging {
  import PersistentReprSerializer._

  def toBinary(x: PersistentRepr): Array[Byte] = {
    val result = for {
      payload <- x.payload.asInstanceOfOpt[AnyRef]
      (name, data) <- bindings.toJson(payload)
    } yield {
      val reprWithJson = ReprWithJson(name, data, x)
      val json = Json.toJson(reprWithJson)
      Json.toBytes(json)
    }
    result getOrElse fallbackSerializer.toBinary(x)
  }

  def fromBinary(bytes: Array[Byte]): PersistentRepr =
    Try(Json.parse(bytes)) match {
      case Success(json) =>
        val reprWithJson = json.as[ReprWithJson]
        val payload = bindings.fromJsonOrError(reprWithJson.`type`, reprWithJson.payload)
        reprWithJson.persistentRepr(payload)

      case Failure(t) =>
        logger.error(s"Error parsing JSON PersistentRepr ${ new String(bytes) }", t)
        val anyRef = fallbackSerializer.fromBinary(bytes, classOf[PersistentRepr])
        anyRef.asInstanceOf[PersistentRepr]
    }

}

object PersistentReprSerializer {
  val UTF8: Charset = Charset.forName("UTF-8")

  case class ReprMetadata(
    nr: Long,
    id: String,
    `type`: String,
    writer: String,
    manifest: String) {

    def persistentRepr(payload: AnyRef): PersistentRepr = PersistentRepr(
      payload = payload,
      sequenceNr = nr,
      persistenceId = id,
      manifest = manifest,
      writerUuid = writer)
  }

  object ReprMetadata {

    implicit val ReprMetadataFormat: OFormat[ReprMetadata] = {
      val format = Json.format[ReprMetadata]
      val reads = format orElse new Reads[ReprMetadata] {
        def reads(json: JsValue) = for {
          nr <- (json \ "nr").validate[Long]
          id <- (json \ "id").validate[String]
          typ <- (json \ "type").validate[String]
          writer <- (json \ "writer").validateOpt[String]
          manifest <- (json \ "manifest").validateOpt[String]
        } yield ReprMetadata(
          nr = nr,
          id = id,
          `type` = typ,
          writer = writer getOrElse PersistentRepr.Undefined,
          manifest = manifest getOrElse PersistentRepr.Undefined)
      }
      OFormat(reads, format)
    }

    def apply(name: String, x: PersistentRepr): ReprMetadata = ReprMetadata(
      nr = x.sequenceNr,
      id = x.persistenceId,
      `type` = name,
      writer = x.writerUuid,
      manifest = if (x.manifest.nonEmpty) x.manifest else x.payload.getClass.getName)
  }


  case class ReprWithJson(
    payload: JsObject,
    nr: Long,
    id: String,
    `type`: String,
    writer: String,
    manifest: String) {

    def persistentRepr(payload: AnyRef): PersistentRepr = PersistentRepr(
      payload = payload,
      sequenceNr = nr,
      persistenceId = id,
      manifest = manifest,
      writerUuid = writer)
  }

  object ReprWithJson {
    implicit val ReprWithJsonFormat: OFormat[ReprWithJson] = {
      val format = Json.format[ReprWithJson]
      val reads = format orElse new Reads[ReprWithJson] {
        def reads(json: JsValue) = for {
          nr <- (json \ "nr").validate[Long]
          id <- (json \ "id").validate[String]
          typ <- (json \ "type").validate[String]
          writer <- (json \ "writer").validateOpt[String]
          manifest <- (json \ "manifest").validateOpt[String]
          payload <- (json \ "payload").validate[JsObject]
        } yield ReprWithJson(
          nr = nr,
          id = id,
          `type` = typ,
          writer = writer getOrElse PersistentRepr.Undefined,
          manifest = manifest getOrElse PersistentRepr.Undefined,
          payload = payload)
      }
      OFormat(reads, format)
    }

    def apply(name: String, json: JsObject, x: PersistentRepr): ReprWithJson = ReprWithJson(
      nr = x.sequenceNr,
      id = x.persistenceId,
      `type` = name,
      writer = x.writerUuid,
      manifest = if (x.manifest.nonEmpty) x.manifest else x.payload.getClass.getName,
      payload = json)
  }
}
