package com.evolutiongaming.serialization

import java.nio.charset.Charset

import play.api.libs.json.Json

class BindingsSerializer(bindings: Bindings) {
  import BindingsSerializer._

  def manifest(x: AnyRef): Option[String] = for {
    binding <- bindings.bindingByValue(x)
  } yield binding.manifests.head

  def toBinary(x: AnyRef): Option[Array[Byte]] = for {
    (name, json) <- bindings.toJson(x)
  } yield {
    val str = Json stringify (json ++ Json.obj("persistenceType" -> name))
    str getBytes UTF8
  }

  def fromBinary(bytes: Array[Byte], manifest: String): Option[AnyRef] = for {
    binding <- bindings.bindingByManifest(manifest)
    json = Json parse bytes
  } yield {
    binding read json recoverTotal { error => sys error s"Cannot deserialize $json as $manifest: $error" }
  }
}

object BindingsSerializer {
  val UTF8: Charset = Charset.forName("UTF-8")
}