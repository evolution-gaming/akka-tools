package com.evolutiongaming.cluster

case class ShardedMsg(id: String, msg: Serializable) {
  require(id != null, "id is null")
  require(msg != null, "msg is null")
}