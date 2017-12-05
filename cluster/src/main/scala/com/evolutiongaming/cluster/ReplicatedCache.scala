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

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.WriteConsistency
import akka.cluster.ddata.{Replicator, _}

import scala.collection.concurrent.TrieMap

class ReplicatedCache[K, V](
  dataKey: LWWMapKey[K, V],
  system: ActorSystem,
  writeConsistency: WriteConsistency = Replicator.WriteLocal) {

  private val cache = TrieMap.empty[K, V]
  private val ref = system.actorOf(Props(new Proxy))

  def get(key: K): Option[V] = {
    cache.get(key)
  }

  def put(key: K, value: V): Unit = {
    ref ! Msg.Put(key, value)
    cache.put(key, value)
  }

  def clear(): Unit = {
    ref ! Msg.Clear
    cache.clear()
  }


  private class Proxy extends Actor with ActorLogging {

    implicit lazy val node = Cluster(context.system)
    lazy val replicator = DistributedData(context.system).replicator

    replicator ! Replicator.Subscribe(dataKey, self)

    def update[T](modify: LWWMap[K, V] => LWWMap[K, V]) = {
      replicator ! Replicator.Update(dataKey, LWWMap.empty[K, V], writeConsistency)(modify)
    }

    def receive: Receive = {
      case msg @ Msg.Put(key, value) =>
        logDebug(msg)
        update { _ + (key -> value) }

      case msg @ Msg.Remove(key) =>
        logDebug(msg)
        update { _ - key }

      case msg @ Msg.Clear =>
        logDebug(msg)
        replicator ! Replicator.Delete(dataKey, writeConsistency)

      case Replicator.UpdateSuccess(`dataKey`, _) =>
        logDebug("UpdateSuccess")

      case Replicator.UpdateTimeout(`dataKey`, _) =>
        log warning s"$dataKey UpdateTimeout"

      case msg @ Replicator.Changed(`dataKey`) =>
        val data = msg.get(dataKey)
        log.debug(s"{} Changed {} entries", dataKey, data.size)
        data.entries foreach { case (k, v) => cache.put(k, v) }

      case Replicator.ReplicationDeleteFailure(`dataKey`, _) =>
        log warning s"$dataKey ReplicationDeleteFailure"

      case Replicator.DeleteSuccess(`dataKey`, _) =>
        logDebug("DeleteSuccess")
        cache.clear()

      case msg => log warning s"$dataKey unexpected $msg"
    }

    private def logDebug(msg: Any) = {
      log.debug(s"{} {}", dataKey, msg)
    }
  }

  private sealed trait Msg

  private object Msg {
    case class Put(key: K, value: V) extends Msg
    case class Remove(key: K) extends Msg
    case object Clear extends Msg
  }
}

object ReplicatedCache {

  def apply[K, V](dataKey: LWWMapKey[K, V], system: ActorSystem): ReplicatedCache[K, V] = {
    new ReplicatedCache[K, V](dataKey, system)
  }
}
