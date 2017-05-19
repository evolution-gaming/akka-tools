package com.evolutiongaming.cluster

import akka.actor.{Address, ExtendedActorSystem, Extension, ExtensionId}

class AddressHelper(val defaultAddress: Address) extends Extension {

  def toLocal(address: Address): Address = {
    if (address == defaultAddress) address.copy(host = None, port = None, protocol = "akka") else address
  }

  def toGlobal(address: Address): Address = {
    if (address.hasGlobalScope) address else defaultAddress
  }
}

object AddressHelperExtension extends ExtensionId[AddressHelper] {
  def createExtension(system: ExtendedActorSystem): AddressHelper = {
    new AddressHelper(system.provider.getDefaultAddress)
  }
}