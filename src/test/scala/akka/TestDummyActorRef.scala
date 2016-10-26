package akka

import akka.actor._

class TestDummyActorRef(val path: ActorPath) extends MinimalActorRef {
  def provider: ActorRefProvider = null
}
