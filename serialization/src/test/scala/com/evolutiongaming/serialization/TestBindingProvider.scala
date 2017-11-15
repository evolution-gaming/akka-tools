package com.evolutiongaming.serialization

class TestBindingProvider extends BindingProvider {
  override def eventBindings = Bindings(Binding[TestTraitB]("TestTrait", TraitsHierarchyFormatter))

  override def snapshotBindings = Bindings(Binding[TestCaseClassA]("TestCaseClassA", TestCaseClassA.JsonFormat))
}
