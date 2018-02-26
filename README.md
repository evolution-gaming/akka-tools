# akka-tools [![Build Status](https://travis-ci.org/evolution-gaming/akka-tools.svg)](https://travis-ci.org/evolution-gaming/akka-tools) [![Coverage Status](https://coveralls.io/repos/evolution-gaming/akka-tools/badge.svg)](https://coveralls.io/r/evolution-gaming/akka-tools) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/c714d1663a2c4e40bcbf868d1d2260cc)](https://www.codacy.com/app/evolution-gaming/akka-tools?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/akka-tools&amp;utm_campaign=Badge_Grade) [ ![version](https://api.bintray.com/packages/evolutiongaming/maven/akka-tools/images/download.svg) ](https://bintray.com/evolutiongaming/maven/akka-tools/_latestVersion) [![License: MIT](https://img.shields.io/badge/License-MIT-yellowgreen.svg)](https://opensource.org/licenses/MIT)

### ExtractShardId
ExtractShardId is an interface of the function used by Akka sharding to extract ShardId from an incoming message.
Our implementation of ExtractShardId supports mapping of specific EntityId-s to specific ShardId-s. 
All other not pre-configured EntityId-s will be mapped to equal individual ShardId-s.

## Setup

```scala
resolvers += Resolver.bintrayRepo("evolutiongaming", "maven")

libraryDependencies += "com.evolutiongaming" %% "akka-tools" % "1.3.18"
```
