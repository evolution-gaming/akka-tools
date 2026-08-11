package akka.persistence

import akka.actor.{ActorRef, Props}
import akka.persistence.JournalProtocol.{DeleteMessagesTo, WriteMessages}
import akka.persistence.SnapshotProtocol.{DeleteSnapshot, DeleteSnapshots, SaveSnapshot}
import akka.testkit.TestProbe
import com.evolutiongaming.test.ActorSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DuplicatingActorSpec extends AnyWordSpec with ActorSpec with Matchers {

  "DuplicatingActor" should {

    "forward WriteMessages to primary journal and replicate to secondary journal" in new TestScope {
      val msg = WriteMessages(Nil, testActor, 0)
      ref ! msg
      primary.expectMsg(msg)
      primary.lastSender shouldEqual testActor
      secondary.expectMsg(msg.copy(persistentActor = logger.ref))
      secondary.lastSender shouldEqual logger.ref
    }

    "forward DeleteMessagesTo to primary journal and replicate to secondary journal" in new TestScope {
      val msg = DeleteMessagesTo("persistenceId", toSequenceNr = 100, testActor)
      ref ! msg
      primary.expectMsg(msg)
      secondary.expectMsg(msg.copy(persistentActor = logger.ref))
    }

    "forward SaveSnapshot to primary journal and replicate to secondary journal" in new TestScope {
      val msg = SaveSnapshot(snapshotMetadata, "snapshot")
      ref ! msg
      primary.expectMsg(msg)
      primary.lastSender shouldEqual testActor
      secondary.expectMsg(msg)
      secondary.lastSender shouldEqual logger.ref
    }

    "forward DeleteSnapshot to primary journal and replicate to secondary journal" in new TestScope {
      val msg = DeleteSnapshot(snapshotMetadata)
      ref ! msg
      primary.expectMsg(msg)
      primary.lastSender shouldEqual testActor
      secondary.expectMsg(msg)
      secondary.lastSender shouldEqual logger.ref
    }

    "forward DeleteSnapshots to primary journal and replicate to secondary journal" in new TestScope {
      val msg = DeleteSnapshots("persistenceId", SnapshotSelectionCriteria())
      ref ! msg
      primary.expectMsg(msg)
      primary.lastSender shouldEqual testActor
      secondary.expectMsg(msg)
      secondary.lastSender shouldEqual logger.ref
    }
  }

  private trait TestScope extends ActorScope {
    val snapshotMetadata = SnapshotMetadata("persistenceId", 0)
    val primary = TestProbe()
    val secondary = TestProbe()
    val logger = TestProbe()

    val ref = system.actorOf(Props(new DuplicatingActor))

    class DuplicatingActor extends com.evolutiongaming.util.DuplicatingActor {

      override val failureLogger: ActorRef = logger.ref

      def actorForId(name: String) = name match {
        case "primary"   => TestScope.this.primary.ref
        case "secondary" => TestScope.this.secondary.ref
      }
      def primaryName = "evolutiongaming.duplicating.test.primary"
      def secondaryName = "evolutiongaming.duplicating.test.secondary"
    }
  }
}
