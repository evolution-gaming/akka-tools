package com.evolutiongaming.cluster

import akka.actor.{Address, ExtendedActorSystem, Extension, ExtensionId}

class AddressHelper(system: ExtendedActorSystem) extends Extension {

  lazy val address: Address = system.provider.getDefaultAddress

  implicit class RichAddress(val self: Address) {
    def local: Address = if (self == address) self.copy(host = None, port = None, protocol = "akka") else self
    def global: Address = if (self.hasGlobalScope) self else address
  }
}

object AddressHelperExtension extends ExtensionId[AddressHelper] {
  def createExtension(system: ExtendedActorSystem): AddressHelper = new AddressHelper(system)
}