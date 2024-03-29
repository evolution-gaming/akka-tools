# akka-tools 
[![Build Status](https://github.com/evolution-gaming/akka-tools/workflows/CI/badge.svg)](https://github.com/evolution-gaming/akka-tools/actions?query=workflow%3ACI) 
[![Coverage Status](https://coveralls.io/repos/evolution-gaming/akka-tools/badge.svg)](https://coveralls.io/r/evolution-gaming/akka-tools)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/c714d1663a2c4e40bcbf868d1d2260cc)](https://www.codacy.com/app/evolution-gaming/akka-tools?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/akka-tools&amp;utm_campaign=Badge_Grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=akka-tools_2.13&repos=public)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

### ExtractShardId
ExtractShardId is an interface of the function used by Akka sharding to extract ShardId from an incoming message.
Our implementation of ExtractShardId supports mapping of specific EntityId-s to specific ShardId-s.
All other not pre-configured EntityId-s will be mapped to equal individual ShardId-s.

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolutiongaming" %% "akka-tools" % "3.0.12"
```
