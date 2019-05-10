package com.evolutiongaming.util.dispatchers

import com.evolutiongaming.config.ConfigHelper._
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
      id = config.get[String]("id"),
      mdc = config.getOpt[Boolean]("mdc") getOrElse false,
      metrics = config.getOpt[Boolean]("metrics") getOrElse false,
      executionTracker = config.getOpt[Config]("execution-tracker") flatMap ExecutionTracker.opt)
  }

  
  case class ExecutionTracker(
    hangingThreshold: FiniteDuration = 10.seconds,
    checkInterval: FiniteDuration = 1.seconds)

  object ExecutionTracker {
    val Default: ExecutionTracker = ExecutionTracker()

    def opt(config: Config): Option[ExecutionTracker] = {
      def executionTracker = ExecutionTracker(
        hangingThreshold = config.getOpt[FiniteDuration]("hanging-threshold") getOrElse Default.hangingThreshold,
        checkInterval = config.getOpt[FiniteDuration]("check-interval") getOrElse Default.checkInterval)

      val enabled = config.getOpt[Boolean]("enabled") getOrElse false
      if (enabled) Some(executionTracker) else None
    }
  }
}