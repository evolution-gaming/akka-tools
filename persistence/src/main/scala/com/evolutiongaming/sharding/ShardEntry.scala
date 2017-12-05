package com.evolutiongaming.sharding

import akka.actor.{ActorPath, ActorRef}

case class ShardId(id: String, typeName: String)

object ShardId {
  def apply(path: ActorPath): ShardId = ShardId(path.name, path.parent.name)
  def apply(ref: ActorRef): ShardId = ShardId(ref.path)
}

case class ShardEntry(id: String, region: ShardId)

object ShardEntry {
  def apply(path: ActorPath): ShardEntry = ShardEntry(path.name, ShardId(path.parent))
  def apply(ref: ActorRef): ShardEntry = ShardEntry(ref.path)
  def apply(id: String, typeName: String): ShardEntry = ShardEntry(id, ShardId(id = "", typeName = typeName))
}