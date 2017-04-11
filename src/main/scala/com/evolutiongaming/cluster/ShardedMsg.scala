package com.evolutiongaming.cluster

import akka.cluster.sharding.ShardRegion

case class ShardedMsg(id: String, msg: Serializable) {
  require(id != null, "id is null")
  require(msg != null, "msg is null")
}

object ShardedMsg {
  val ExtractEntityId: ShardRegion.ExtractEntityId = {
    case ShardedMsg(id, msg) => (id, msg)
  }
}