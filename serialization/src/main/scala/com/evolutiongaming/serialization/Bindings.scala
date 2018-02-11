package com.evolutiongaming.serialization

import com.evolutiongaming.nel.Nel
import com.github.t3hnar.scalax._
import play.api.libs.json._

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

object Bindings {

  def apply(binding: Binding[_ <: AnyRef], bindings: Binding[_ <: AnyRef]*): Bindings = {
    Bindings(binding :: bindings.toList)
  }


  private case class CachedCall[K, V] private(key: K => Any, f: K => V) extends (K => V) {
    private val cache = TrieMap.empty[Any, V]

    def apply(k: K): V = cache.getOrElseUpdate(key(k), f(k))
  }

  private object CachedCall {
    def apply[K, V](f: K => V): CachedCall[K, V] = CachedCall(identity, f)
    def by[K, V](key: K => Any)(f: K => V): CachedCall[K, V] = CachedCall(key, f)
  }
}


case class Bindings(bindings: List[Binding[_ <: AnyRef]]) {
  import Bindings.CachedCall

  def +(that: Bindings) = Bindings(bindings = bindings ++ that.bindings)


  lazy val bindingByName: (String => Option[Binding[AnyRef]]) = CachedCall { name =>
    bindings collectFirst { case binding if binding.name == name =>
      binding.asInstanceOf[Binding[AnyRef]]
    }
  }

  lazy val bindingByManifest: (String => Option[Binding[AnyRef]]) = CachedCall { manifest =>
    bindings collectFirst {
      case binding if binding.manifests contains manifest => binding.asInstanceOf[Binding[AnyRef]]
    }
  }

  lazy val bindingByValue: (AnyRef => Option[Binding[AnyRef]]) = CachedCall.by[AnyRef, Option[Binding[AnyRef]]](_.getClass) { anyRef =>
    bindings collectFirst {
      case binding if binding.unapply(anyRef).isDefined => binding.asInstanceOf[Binding[AnyRef]]
    }
  }

  def fromJson(name: String, json: JsValue): Option[JsResult[AnyRef]] = {
    for {binding <- bindingByName(name)} yield binding read json
  }

  def toJson(anyRef: AnyRef): Option[(String, JsObject)] = for {
    binding <- bindingByValue(anyRef)
    json <- binding.toJson(anyRef)
  } yield binding.name -> json

  def fromJsonOrError(name: String, json: JsValue): AnyRef = {
    val result = fromJson(name, json) orError s"No binding found for $name to read $json"
    result recoverTotal { error => sys error s"Cannot deserialize $name from $json: $error" }
  }
}

case class Binding[T](name: String, private val format: Format[T], manifests: Nel[String])
  (implicit val tag: ClassTag[T]) {

  def toJson(anyRef: AnyRef): Option[JsObject] = {
    for {anyRef <- tag.unapply(anyRef)} yield {
      val jsValue = format.writes(anyRef)
      jsValue.as[JsObject]
    }
  }

  def read(json: JsValue): JsResult[T] = format reads json

  def unapply(anyRef: AnyRef): Option[T] = tag unapply anyRef
}

object Binding {

  def apply[T](name: String, format: Format[T], extraManifests: String*)(implicit tag: ClassTag[T]): Binding[T] = {
    Binding(name, format, Nel(tag.runtimeClass.getName, extraManifests.toList))
  }
}