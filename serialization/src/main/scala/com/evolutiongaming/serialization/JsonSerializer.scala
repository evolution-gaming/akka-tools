package com.evolutiongaming.serialization

import java.nio.ByteBuffer

import akka.actor.ExtendedActorSystem
import akka.persistence.PersistentRepr
import akka.persistence.serialization.Snapshot
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import com.twitter.chill.akka.AkkaSerializer
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.github.t3hnar.scalax.RichOption

class JsonSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest with LazyLogging {

  private val config: Config = system.settings.config

  private val providerClassName = config.getString("evolutiongaming.serialization.binding-provider")

  val bindingProvider: BindingProvider = Class.forName(providerClassName).newInstance().asInstanceOf[BindingProvider]
  
  import JsonSerializer._

  private lazy val binarySerializer: akka.serialization.Serializer = {
    val serialization = SerializationExtension(system)
    val serializer = serialization.serializerByIdentity.get(8675309) // com.twitter.chill.akka.AkkaSerializer.identifier
    serializer getOrElse {
      logger.warn(s"${classOf[AkkaSerializer].getName} not found, using JavaSerializer instead")
      SerializerOf[akka.serialization.JavaSerializer](system)
    }
  }

  private lazy val persistentReprSerializer = {
    val default = SerializerOf[akka.persistence.serialization.MessageSerializer](system)
    new PersistentReprSerializer(bindingProvider.eventBindings, default)
  }

  private lazy val snapshotSerializer = {
    val default = SerializerOf[akka.persistence.serialization.SnapshotSerializer](system)
    new SnapshotJsonSerializer(bindingProvider.snapshotBindings, fallbackSerializer = default, binarySerializer = binarySerializer)
  }

  private lazy val bindingsSerializer = new BindingsSerializer(bindingProvider.eventBindings + bindingProvider.snapshotBindings)


  def identifier: Int = Identifier

  def manifest(x: AnyRef): String = x match {
    case _: PersistentRepr => PersistentReprManifest
    case _: Snapshot       => SnapshotManifest
    case _                 => bindingsSerializer.manifest(x) orError s"No manifest defined for ${x.getClass.getName}"
  }


  def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case PersistentReprManifest => persistentReprSerializer.fromBinary(bytes)
    case SnapshotManifest       => snapshotSerializer.fromBinary(bytes)
    case ""                     => sys error "Cannot deserialize without manifest"
    case _                      => bindingsSerializer.fromBinary(bytes, manifest) orError s"Cannot deserialize $manifest"
  }

  def toBinary(x: AnyRef): Array[Byte] = x match {
    case x: PersistentRepr => persistentReprSerializer.toBinary(x)
    case x: Snapshot       => snapshotSerializer.toBinary(x)
    case _                 => bindingsSerializer.toBinary(x) orError s"Cannot serialize ${x.getClass.getName}"
  }
}

object JsonSerializer {
  val Identifier: Int = ByteBuffer.wrap("json" getBytes "UTF-8").getInt

  val PersistentReprManifest: String = classOf[PersistentRepr].getName
  val SnapshotManifest      : String = classOf[Snapshot].getName
}
