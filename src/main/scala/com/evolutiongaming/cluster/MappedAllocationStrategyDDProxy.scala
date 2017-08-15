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

class MappedAllocationStrategyDDProxy extends Actor with ActorLogging {

  import MappedAllocationStrategy._
  import MappedAllocationStrategyDDProxy._

  implicit lazy val node = Cluster(context.system)
  lazy val replicator: ActorRef = DistributedData(context.system).replicator
  val emptyMap = LWWMap.empty[String, ActorRef]

  replicator ! Subscribe(MappingKey, self)

  def receive: Receive = {
    case UpdateMapping(typeName, id, regionRef) =>
      val entityKey = EntityKey(typeName, id)
      shardToRegionMapping += entityKey -> regionRef
      replicator ! Update(MappingKey, emptyMap, WriteLocal)(_ + (entityKey.toString -> regionRef))

    case Clear(typeName, id) =>
      val entityKey = EntityKey(typeName, id)
      shardToRegionMapping -= entityKey
      replicator ! Update(MappingKey, emptyMap, WriteLocal)(_ - entityKey.toString)

    case UpdateSuccess(_, _) =>

    case UpdateTimeout(key, _) =>
      // probably should never happen for local updates
      log warning s"Update timeout for key $key"

    case c@Changed(MappingKey) =>
      val newData = (c get MappingKey).entries flatMap {
        case (key, ref) =>
          EntityKey unapply key map { entityKey =>
            entityKey -> ref
          }
      }
      shardToRegionMapping = newData
  }
}

object MappedAllocationStrategyDDProxy extends ExtensionId[ActorRefExtension] {
  override def createExtension(system: ExtendedActorSystem): ActorRefExtension =
    new ActorRefExtension(system actorOf Props[MappedAllocationStrategyDDProxy])

  // DData key of ShardToRegionMapping map
  private[cluster] val MappingKey: LWWMapKey[String, ActorRef] =
    LWWMapKey[String, ActorRef]("ShardToRegionMapping")
}
