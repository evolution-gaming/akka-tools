package com.evolutiongaming.util.dispatchers

import com.evolutiongaming.util.ConfigHelper._
import com.typesafe.config.Config

import scala.concurrent.duration._

case class InstrumentedConfig(
  id: String,
  mdc: Boolean,
  metrics: Boolean,
  executionTracker: Option[InstrumentedConfig.ExecutionTracker])

object InstrumentedConfig {
  def apply(config: Config): InstrumentedConfig = {
    InstrumentedConfig(
      id = config.getString("id"),
      mdc = config.getOpt[Boolean]("mdc") getOrElse false,
      metrics = config.getOpt[Boolean]("metrics") getOrElse true,
      executionTracker = config getOpt[Config] "execution-tracker" flatMap ExecutionTracker.opt)
  }

  case class ExecutionTracker(hangingThreshold: FiniteDuration = 10.seconds)

  object ExecutionTracker {
    val Default: ExecutionTracker = ExecutionTracker()

    def opt(config: Config): Option[ExecutionTracker] = {
      def executionTracker = ExecutionTracker(
        hangingThreshold = config.getOpt[FiniteDuration]("hanging-threshold") getOrElse Default.hangingThreshold)

      val enabled = config.getOpt[Boolean]("enabled") getOrElse false
      if (enabled) Some(executionTracker) else None
    }
  }
}