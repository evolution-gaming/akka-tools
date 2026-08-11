package com.evolutiongaming.serialization

import com.evolutiongaming.serialization.BrokenSerializer._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BrokenSerializerSpec extends AnyWordSpec with Matchers {
  "BrokenSerializer" when {

    "toBinary" should {
      "throw exception on FailTo.Serialize" in {
        the[FailToSerializeException] thrownBy serializer.toBinary(failToSerialize)
      }

      "convert FailTo.Deserialize to binary" in {
        serializer.toBinary(failToDeserialize) shouldEqual bytes
      }

      "throw exception on unknown types" in {
        the[RuntimeException] thrownBy serializer.toBinary("unknown")
      }
    }

    "fromBinary" should {
      "throw exception on FailTo.Deserialize" in {
        the[FailToDeserializeException] thrownBy serializer.fromBinary(bytes, FailTo.Deserialize.Manifest)
      }

      "throw exception on unknown types" in {
        the[RuntimeException] thrownBy serializer.fromBinary(bytes, "test")
      }

      "parse FailTo.Serialize from binary" in {
        serializer.fromBinary("test".getBytes("UTF-8"), FailTo.Serialize.Manifest) shouldEqual failToSerialize
      }
    }

    "manifest" should {
      "return manifest for FailTo.Serialize" in {
        serializer.manifest(failToSerialize) shouldEqual FailTo.Serialize.Manifest
      }

      "return manifest for FailTo.Deserialize" in {
        serializer.manifest(failToDeserialize) shouldEqual FailTo.Deserialize.Manifest
      }

      "throw exception on unknown types" in {
        the[RuntimeException] thrownBy serializer.manifest("unknown")
      }
    }
  }

  lazy val serializer = new BrokenSerializer()
  lazy val bytes = "test" getBytes "UTF-8"
  lazy val failToSerialize = FailTo.Serialize("test")
  lazy val failToDeserialize = FailTo.Deserialize("test")
}
