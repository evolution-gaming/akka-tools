package com.evolutiongaming.serialization

import java.time.Instant

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.serialization.Snapshot
import akka.util.ByteString
import com.evolutiongaming.test.ActorSpec
import org.scalatest.{Matchers, WordSpec}
import play.api.libs.json.Json

class JsonSerializerSpec extends WordSpec with ActorSpec with Matchers {
  "JsonSerializer" should {

    "serialize and deserialize PersistentRepr as json" in new Scope {
      val repr = persistentRepr(realityChecked)
      val bytes = serializer.toBinary(repr)

      serializer.fromBinary(bytes, classOf[PersistentRepr]) shouldEqual repr
      parse(bytes) shouldEqual parse("PersistentRepr.json")
    }


    "serialize and deserialize TestTraitB with manifest" in new Scope {
      val bytes = serializer.toBinary(realityChecked)
      val actual = serializer.fromBinary(bytes, Some(classOf[TestTraitB]))
      realityChecked shouldEqual actual
    }

    "serialize and deserialize TestCaseClassA with manifest" in new Scope {
      val expected = TestCaseClassA.Test
      val bytes = serializer.toBinary(expected)
      val actual = serializer.fromBinary(bytes, Some(classOf[TestCaseClassA]))
      expected shouldEqual actual
    }

    "serialize and deserialize Snapshot with manifest" in new Scope {
      val expected = Snapshot(TestCaseClassA.Test)
      val bytes = serializer.toBinary(expected)
      val actual = serializer.fromBinary(bytes, classOf[Snapshot])
      expected shouldEqual actual
    }

    "serialize and deserialize Snapshot's payload" in new Scope {
      val expected = TestCaseClassA.Test
      val bytes = serializer.toBinary(expected)
      val actual = serializer.fromBinary(bytes, classOf[TestCaseClassA])
      expected shouldEqual actual
    }
  }

  private trait Scope {
    val timestamp = Instant.parse("2015-04-29T11:11:00.000Z")
    val realityChecked = TestCaseClassB("userId", timestamp)
    val serializer = new JsonSerializer(system.asInstanceOf[ExtendedActorSystem])
    val binary = "binary"

    def persistentRepr(payload: Any) = PersistentRepr(
      payload = payload,
      sequenceNr = 3,
      persistenceId = "persistenceId",
      manifest = payload.getClass.getName,
      writerUuid = "writerUuid")

    def parse(name: String) = {
      Json parse (getClass getResourceAsStream name)
    }

    def parse(bs: ByteString) = Json parse bs.utf8String

    def parse(bs: Array[Byte]) = Json parse bs
  }
}