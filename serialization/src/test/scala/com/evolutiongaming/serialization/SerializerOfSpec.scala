package com.evolutiongaming.serialization

import akka.serialization.JavaSerializer
import com.evolutiongaming.test.ActorSpec
import org.scalatest.{Matchers, WordSpec}

class SerializerOfSpec extends WordSpec with Matchers with ActorSpec {
  "SerializerOf" should {
    "find serializer of type" in new ActorScope {
      val serializer = SerializerOf[JavaSerializer](system)
      serializer should not be null
    }
  }
}
