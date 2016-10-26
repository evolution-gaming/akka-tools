package com.evolutiongaming.cluster

@SerialVersionUID(1)
case class ClusterMsg(id: String, msg: Serializable) {
  require(id != null, "id is null")
  require(msg != null, "msg is null")
}