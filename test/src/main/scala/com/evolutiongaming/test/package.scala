package com.evolutiongaming

import akka.actor.Actor.Receive

package object test {

  implicit class ReceiveOps(val self: Receive) {
    def and(receive: Receive): Receive = new Receive {
      def isDefinedAt(x: Any): Boolean = {
        (self isDefinedAt x) || (receive isDefinedAt x)
      }

      def apply(x: Any): Unit = {
        self lift x
        receive lift x
      }
    }
  }
}