# akka-tools [![Build Status](https://travis-ci.org/evolution-gaming/akka-tools.svg)](https://travis-ci.org/evolution-gaming/akka-tools) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/akka-tools/badge.svg)](https://coveralls.io/r/evolution-gaming/akka-tools) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/akka-tools/images/download.svg) ](https://bintray.com/evolutiongaming/maven/akka-tools/_latestVersion)

### ExtractShardId
ExtractShardId is an interface of the function used by Akka sharding to extract ShardId from an incoming message.
Our implementation of ExtractShardId supports mapping of specific EntityId-s to specific ShardId-s. 
All other not pre-configured EntityId-s will be mapped to equal individual ShardId-s.

### SingleNodeAllocationStrategy
SingleNodeAllocationStrategy is used to allocate all the shards on a node which is currently considered as the Master of our cluster (it may differ from the Akka Cluster's Leader).

### AdaptiveAllocationStrategy
AdaptiveAllocationStrategy is used to allocate a shard on a node with the maximum amount of traffic (messages to this shard from this node) during specified time interval.

### DualAllocationStrategy
DualAllocationStrategy is a proxy-like allocation strategy for using two different allocation strategies simultaneously.
It has a list of ShardId-s which should be processed by the second (additional) allocation strategy (this setting is dynamic and can be reconfigured on the fly).
All other shards will be processed by the first (base) allocation strategy.

### DirectAllocationStrategy
DirectAllocationStrategy allows to configure which shard should be allocated on which node (this setting is dynamic and can be reconfigured on the fly).
It uses fallback allocation strategy for processing of 
a) shards for which their nodes are not configured;
b) shards if their nodes are not found in the current cluster nodes.

## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "akka-tools" % "1.2.4"
```