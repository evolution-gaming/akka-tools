package akka.persistence

import java.util.UUID

import akka.actor.{ActorLogging, Props}
import com.evolutiongaming.serialization.BrokenSerializer
import com.evolutiongaming.util.ActorSpec
import org.scalatest.{Matchers, ParallelTestExecution, WordSpec}

import scala.concurrent.duration._

class RecoveryBreakerSpec extends WordSpec with ActorSpec with Matchers with ParallelTestExecution {

  "RecoveryBreaker" should {

    "not stop recovery if not replied too many events" in new TestScope {
      persistEvent("1")
      recover("1")
      expectMsg(RecoveryCompleted)
      persistEvent("2")
    }

    "stop recovery if replied too many events" in new TestScope {
      persistEvent("1")
      persistEvent("2")
      persistEvent("3")

      recover("1", "2")
      expectTerminated(ref)
      ref = newRef()
      ref ! "4"
      ref ! "5"
      expectMsg(RecoveryCompleted)
      expectMsgAllOf("4", "5")
    }

    // looks like something has been fixed in akka
    "stop recovery fail to deserialize snapshot" ignore new TestScope {
      persistEvent("1")
      saveSnapshot(BrokenSerializer.FailTo.Deserialize("1"))
      recover("1")
      expectMsg(RecoveryCompleted)
      persistEvent("2")
    }
  }

  private trait TestScope extends ActorScope {
    val persistenceId = UUID.randomUUID().toString

    var ref = {
      val ref = newRef()
      expectMsg(RecoveryCompleted)
      ref
    }

    def recover(xs: String*): Unit = {
      watch(ref)
      system.stop(ref)
      expectTerminated(ref)
      ref = newRef()
      expectMsgAllOf(xs: _*)
    }

    def newRef() = {
      val ref = system.actorOf(Props(new TestActor(persistenceId)))
      watch(ref)
    }

    def persistEvent(x: String): Unit = {
      ref ! x
      expectMsg(x)
    }

    def saveSnapshot(snapshot: Any): Unit = {
      ref ! SaveSnapshot(snapshot)
      expectMsgType[SaveSnapshotSuccess]
    }

    class TestActor(val persistenceId: String) extends akka.persistence.PersistentActor with ActorLogging {

      private lazy val recoveryBreaker = {
        import RecoveryBreaker.Action
        RecoveryBreaker(
          actor = this,
          saveSnapshotOncePer = 2,
          allowedNumberOfEvents = 2,
          action = Action.Clear(3.seconds),
          replayDelay = 0.seconds) {
          envelopes => for {envelope <- envelopes} envelope.sender ! envelope.message
        }
      }

      def receiveCommand = {
        case SaveSnapshot(snapshot) => saveSnapshot(snapshot)
        case x: SaveSnapshotSuccess => testActor ! x
        case x: SaveSnapshotFailure => testActor ! x
        case x: String              => persist(x) {sender() ! _}
      }

      def receiveRecover = {
        case RecoveryCompleted                  => testActor ! RecoveryCompleted
        case SnapshotOffer(_, snapshot: String) => testActor ! snapshot
        case x: String                          =>
          testActor ! x
          recoveryBreaker.onEventRecover(lastSequenceNr)
      }

      override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]) = {
        super.onRecoveryFailure(cause, event)
        recoveryBreaker.onRecoveryFailure(cause)
      }
    }
  }
  case class SaveSnapshot(snapshot: Any)
}
