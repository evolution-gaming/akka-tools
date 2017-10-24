package com.evolutiongaming.test

import com.typesafe.config.{Config, ConfigFactory}

object TestConfig {

  private lazy val value = ConfigFactory.load("test-akka.conf")

  def apply(): Config = value
}