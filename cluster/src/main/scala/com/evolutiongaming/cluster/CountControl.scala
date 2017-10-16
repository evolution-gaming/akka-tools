package com.evolutiongaming.cluster

import akka.cluster.sharding.ShardRegion

object CountControl {
  type Msg = ShardRegion.Msg
  type Weight = Int
  type Type = Msg => Weight

  val Empty: Type = _ => 0
  val Increment: Type = _ => 1
}
