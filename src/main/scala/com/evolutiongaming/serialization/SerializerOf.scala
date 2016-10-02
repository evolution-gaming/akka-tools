package com.evolutiongaming.serialization

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension

import scala.reflect.ClassTag

object SerializerOf {
  def apply[T <: akka.serialization.Serializer](system: ActorSystem)(implicit tag: ClassTag[T]): T = {
    val config = system.settings.config.getConfig("akka.actor.serialization-identifiers")
    val name = tag.runtimeClass.getName
    val identifier = config.getInt(s""""$name"""")
    val serializer = SerializationExtension(system).serializerByIdentity(identifier)
    serializer.asInstanceOf[T]
  }
}