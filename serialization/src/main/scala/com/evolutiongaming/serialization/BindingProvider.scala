package com.evolutiongaming.serialization

trait BindingProvider {
  def eventBindings: Bindings

  def snapshotBindings: Bindings
}

final class EmptyBindingProvider extends BindingProvider {
  val eventBindings = Bindings(List.empty)
  val snapshotBindings = Bindings(List.empty)
}