package com.evolutiongaming.cluster

import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.cluster.sharding.ShardRegion
import com.typesafe.scalalogging.slf4j.LazyLogging

trait ExtendedShardAllocationStrategy extends ShardAllocationStrategy with LazyLogging {

  def extractShardId(numberOfShards: Int): ShardRegion.ExtractShardId = {
    case x: ClusterMsg =>
      val id = x.id
      val shardId = math.abs(id.hashCode % numberOfShards).toString
      shardId
  }
}
