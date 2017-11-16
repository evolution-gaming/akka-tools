package com.evolutiongaming.replicate2kafka

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId}
import com.evolutiongaming.util.Validation.{V, _}
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.ClassTag

class ReplicateToKafkaExt(val system: ExtendedActorSystem) extends Extension with LazyLogging {
  var instance: V[ReplicateJournalToKafka] = undefined[ReplicateJournalToKafka]

  def replicateJournalToKafka: V[ReplicateJournalToKafka] = instance

  def start(replicateTypes: List[String]): Unit = {
    instance = ReplicateJournalToKafka(system, replicateTypes).ok
  }
  
  private def undefined[T](implicit tag: ClassTag[T]): V[T] = {
    s"${ tag.runtimeClass.getName } is not defined".ko
  }
}

object ReplicateToKafkaExt extends ExtensionId[ReplicateToKafkaExt] {
  def createExtension(system: ExtendedActorSystem): ReplicateToKafkaExt = new ReplicateToKafkaExt(system)
}