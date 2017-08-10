/*
 * Copyright 2016-2017 Evolution Gaming Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolutiongaming.cluster

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, ExtensionId, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._

import scala.compat.Platform

class AdaptiveAllocationStrategyDistributedDataProxy extends Actor with ActorLogging {

  import AdaptiveAllocationStrategy._
  import AdaptiveAllocationStrategyDistributedDataProxy._

  implicit lazy val node = Cluster(context.system)
  private val selfAddress = node.selfAddress.toString
  lazy val replicator: ActorRef = DistributedData(context.system).replicator
  private val emptyMap = ORMultiMap.empty[String, String]

  replicator ! Subscribe(EntityToNodeCountersKey, self)

  def sendBindingUpdate(entityKey: String, counterKey: String): Unit = {
    replicator ! Update(EntityToNodeCountersKey, emptyMap, WriteLocal)(_ addBinding(entityKey, counterKey))
  }

  def receive: Receive = {
    case Increase(typeName, id, weight) =>
      val entityKey = EntityKey(typeName, id)
      val counterKey = CounterKey(entityKey, selfAddress)
      counters get counterKey match {
        case None =>
          counters += (counterKey -> ValueData(weight.toLong, Platform.currentTime)) // here the value is not cumulative, just eventually consistent
          replicator ! Subscribe(PNCounterKey(counterKey.toString), self)
        case _    =>
      }
      replicator ! Update(PNCounterKey(counterKey.toString), PNCounter.empty, WriteLocal)(_ + weight.toLong)

      entityToNodeCounters get entityKey match {
        case Some(counterKeys) if counterKeys contains counterKey =>

        case Some(counterKeys)                                    =>
          entityToNodeCounters += (entityKey -> (counterKeys + counterKey))
          sendBindingUpdate(entityKey.toString, counterKey.toString)

        case None                                                 =>
          entityToNodeCounters += (entityKey -> Set(counterKey))
          sendBindingUpdate(entityKey.toString, counterKey.toString)
      }

    case Clear(typeName, id) =>
      val entityKey = EntityKey(typeName, id)
      entityToNodeCounters get entityKey match {
        case Some(counterKeys) =>
          for (counterKey <- counterKeys) {
            counters get counterKey match {
              case None    =>
                replicator ! Subscribe(PNCounterKey(counterKey.toString), self)

              case Some(v) =>
                if (v.value.isValidLong) {
                  counters += (counterKey -> ValueData(0, Platform.currentTime))
                  replicator ! Update(PNCounterKey(counterKey.toString), PNCounter.empty, WriteLocal)(_ - v.value.longValue())
                } else {
                  // probably should never happen
                  log warning s"Can't clear counter for key $counterKey - it's bigger than Long"
                }
            }
          }
        case None              =>
      }

    case UpdateSuccess(_, _) =>

    case UpdateTimeout(key, _) =>
      // probably should never happen for local updates
      log warning s"Update timeout for key $key"

    case c@Changed(key: PNCounterKey) =>
      CounterKey unapply key._id match {
        case Some(counterKey) =>
          val newValue = (c get key).value
          counters get counterKey match {
            case None           =>
              counters += (counterKey -> ValueData(newValue, Platform.currentTime))
            case Some(oldValue) =>
              counters += (counterKey -> ValueData(newValue, oldValue.cleared))
          }
        case None             =>
          log error s"Wrong CounterKey: ${ key._id }"
      }

    case c@Changed(EntityToNodeCountersKey) =>
      val newData = (c get EntityToNodeCountersKey).entries flatMap {
        case (key, values) =>
          EntityKey unapply key map { entityKey =>
            entityKey -> (values flatMap { CounterKey.unapply(_) })
          }
      }
      val oldCounterKeys = entityToNodeCounters.values.flatten.toSet
      entityToNodeCounters = newData
      val newCounterKeys = newData.values.flatten.toSet
      val diffKeys = newCounterKeys -- oldCounterKeys
      for (key <- diffKeys) replicator ! Subscribe(PNCounterKey(key.toString), self)
  }
}

object AdaptiveAllocationStrategyDistributedDataProxy extends ExtensionId[ActorRefExtension] {
  override def createExtension(system: ExtendedActorSystem): ActorRefExtension =
    new ActorRefExtension(system actorOf Props[AdaptiveAllocationStrategyDistributedDataProxy])

  // DData key of entityToNodeCounters map
  private[cluster] val EntityToNodeCountersKey: ORMultiMapKey[String, String] =
    ORMultiMapKey[String, String]("EntityToNodeCounters")
}
