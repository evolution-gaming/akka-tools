package com.evolutiongaming.cluster

import akka.actor.{Address, ExtendedActorSystem, Extension, ExtensionId}
import com.evolutiongaming.util.ConfigHelper._
import com.typesafe.config.Config

class AddressHelper(system: ExtendedActorSystem) extends Extension {

  lazy val tcpAddress: Address = {
    val (host, port) = {
      val conf = system.settings.config.get[Config]("akka.remote.netty.tcp")
      val result = for {
        host <- conf.get[Option[String]]("hostname")
        port <- conf.get[Option[Int]]("port")
      } yield (host, port)

      result getOrElse ("127.0.0.1" -> 2552)
    }

    Address(
      protocol = "akka.tcp",
      system = system.name,
      host = host,
      port = port)
  }

  implicit class RichAddress(val self: Address) {
    def local: Address = if (self == tcpAddress) self.copy(host = None, port = None, protocol = "akka") else self
    def global: Address = if (self.hasGlobalScope) self else tcpAddress
  }
}

object AddressHelperExtension extends ExtensionId[AddressHelper] {
  def createExtension(system: ExtendedActorSystem): AddressHelper = new AddressHelper(system)
}