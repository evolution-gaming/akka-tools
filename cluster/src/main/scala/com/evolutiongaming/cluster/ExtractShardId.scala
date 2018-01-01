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

import akka.cluster.sharding.ShardRegion
import com.evolutiongaming.util.ConfigHelper._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

object ExtractShardId extends LazyLogging {

  val identity: ShardRegion.ExtractShardId = {
    case x: ShardedMsg               => x.id
    case ShardRegion.StartEntity(id) => id
  }

  def uniform(numberOfShards: Int): ShardRegion.ExtractShardId = {
    def shardId(entityId: ShardRegion.EntityId): ShardRegion.ShardId =
      math.abs(entityId.hashCode % numberOfShards).toString

    {
      case x: ShardedMsg                     => shardId(x.id)
      case ShardRegion.StartEntity(entityId) => shardId(entityId)
    }
  }

  def static(
    typeName: String,
    config: Config,
    fallback: ShardRegion.ExtractShardId = identity): ShardRegion.ExtractShardId = {
    // "shardId: entityId1, entityId2, entityId3"
    val mappingsList = Try(config.get[List[String]](s"$typeName.id-shard-mapping")) getOrElse List.empty[String]
    val mappingsPairList = mappingsList flatMap { mapping =>
      (mapping split ":").toList match {
        case shardId :: entityIds :: Nil if shardId.trim.nonEmpty && entityIds.trim.nonEmpty =>
          val shardIdTrimmed = shardId.trim
          (entityIds split ",").toList map (_.trim) filter (_.nonEmpty) map (_ -> shardIdTrimmed)
        case other =>
          logger error s"Invalid mapping for $typeName: $other"
          List.empty
      }
    }
    val mappings = mappingsPairList.toMap
    logger debug s"$typeName mappings: $mappings"

    def shardId(entityId: ShardRegion.EntityId, msg: ShardRegion.Msg): ShardRegion.ShardId =
      mappings get entityId match {
        case Some(shardId) => shardId
        case None          => fallback(msg)
      }

    {
      case msg: ShardedMsg                       => shardId(msg.id, msg)
      case msg@ShardRegion.StartEntity(entityId) => shardId(entityId, msg)
    }
  }
}