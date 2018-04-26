package com.evolutiongaming.cluster

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class ExtractShardIdSpec extends FlatSpec
  with Matchers {

  "ExtractShardId.identity" should "map EntityId directly to ShardId" in new Scope {
    ExtractShardId.identity(msg("id")) shouldBe "id"
  }

  "ExtractShardId.identityCustom" should "map EntityId directly to ShardId with custom transformation" in new Scope {
    ExtractShardId.identityCustom(_.split("-", 2).head)(msg("id")) shouldBe "id"
    ExtractShardId.identityCustom(_.split("-", 2).head)(msg("id-blahblah")) shouldBe "id"
    ExtractShardId.identityCustom(_.split("-", 2).head)(msg("id----asdg---")) shouldBe "id"
  }

  "ExtractShardId.uniform" should "uniformly distribute EntityId-s across predefined number of ShardId-s" in new Scope {
    val extractShardId = ExtractShardId.uniform(10)
    extractShardId(msg("1")) shouldBe "9"
    extractShardId(msg("2")) shouldBe "0"
    extractShardId(msg("3")) shouldBe "1"
    extractShardId(msg("4")) shouldBe "2"
    extractShardId(msg("5")) shouldBe "3"
    extractShardId(msg("6")) shouldBe "4"
    extractShardId(msg("7")) shouldBe "5"
    extractShardId(msg("8")) shouldBe "6"
    extractShardId(msg("9")) shouldBe "7"
    extractShardId(msg("0")) shouldBe "8"
  }

  "ExtractShardId.static" should "use static pre-defined EntityId-to-ShardId distribution with fallback" in new Scope {
    val extractShardId = ExtractShardId.static(
      typeName = "typeName",
      config = ConfigFactory parseString
        """typeName.id-shard-mapping = [
          |"shardId1: entityId1, entityId2, entityId3",
          |"shardId2: entityId4, entityId5, entityId6"]""".stripMargin,
      fallback = ExtractShardId.uniform(10))
    extractShardId(msg("entityId1")) shouldBe "shardId1"
    extractShardId(msg("entityId2")) shouldBe "shardId1"
    extractShardId(msg("entityId3")) shouldBe "shardId1"
    extractShardId(msg("entityId4")) shouldBe "shardId2"
    extractShardId(msg("entityId5")) shouldBe "shardId2"
    extractShardId(msg("entityId6")) shouldBe "shardId2"
    extractShardId(msg("entityId7")) shouldBe "9"
    extractShardId(msg("entityId8")) shouldBe "8"
    extractShardId(msg("entityId9")) shouldBe "7"
  }

  private trait Scope {
    def msg(id: String) = ShardedMsg(id, new Serializable {})
  }
}