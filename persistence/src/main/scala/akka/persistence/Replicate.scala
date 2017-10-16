package akka.persistence

import akka.actor.ActorRef
import akka.persistence.JournalProtocol._
import akka.persistence.SnapshotProtocol._

object Replicate {
  def opt(x: Any, ref: ActorRef): Option[Any] = PartialFunction.condOpt(x) {
    case x: DeleteMessagesTo   => x.copy(persistentActor = ref)
    case x: WriteMessages      => x.copy(persistentActor = ref)
    case x: SaveSnapshot       => x
    case x: DeleteSnapshot     => x
    case x: DeleteSnapshots    => x
  }
}
