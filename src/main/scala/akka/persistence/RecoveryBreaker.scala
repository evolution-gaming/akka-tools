package akka.persistence

import akka.actor.{ActorLogging, DiagnosticActorLogging, StashSupport}
import akka.dispatch.Envelope
import akka.event.LoggingAdapter
import akka.pattern.FutureRef
import akka.persistence.JournalProtocol.DeleteMessagesTo
import akka.persistence.SnapshotProtocol.DeleteSnapshots
import com.github.t3hnar.scalax._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.control.NoStackTrace

trait RecoveryBreaker {
  def onRecoveryFailure(cause: Throwable): Unit
  def onEventRecover(sequenceNr: Long): Unit
}

private class RecoveryBreakerImp(
  saveSnapshotOncePer: Long,
  allowedNumberOfEvents: Long,
  actor: PersistentActor,
  action: => RecoveryBreaker.Action,
  replay: Iterable[Envelope] => Any,
  replayDelay: FiniteDuration) extends RecoveryBreaker {

  import RecoveryBreaker._
  import actor.context.{dispatcher, system}

  require(
    allowedNumberOfEvents >= saveSnapshotOncePer,
    s"allowedNumberOfEvents < saveSnapshotOncePer, $allowedNumberOfEvents < $saveSnapshotOncePer")

  private var numberOfEvents: Int = 0

  def onRecoveryFailure(cause: Throwable) = cause match {
    case cause: BreakRecoveryException =>
    case _                             => action match {
      case Action.Clear(timeout) => clearAndReplay(timeout)
      case Action.Stop           =>
      case Action.Ignore         =>
    }
  }

  def onEventRecover(sequenceNr: Long): Unit = {
    numberOfEvents += 1

    def logMsg = s"${actor.persistenceId}($sequenceNr) recovered $numberOfEvents events, while save-snapshot-once-per=$saveSnapshotOncePer"

    if (numberOfEvents == allowedNumberOfEvents) {
      val msg = s"$logMsg: action: ${action.getClass.simpleName}"

      def onClear(timeout: FiniteDuration) = {
        clearAndReplay(timeout)
        throw new BreakRecoveryException(msg)
      }

      action match {
        case Action.Clear(timeout) => onClear(timeout)
        case Action.Stop           => throw new BreakRecoveryException(msg)
        case Action.Ignore         => log.error(msg)
      }
    } else if (numberOfEvents == saveSnapshotOncePer) {
      log.warning(logMsg)
    }
  }

  private def log: LoggingAdapter = actor match {
    case actor: ActorLogging           => actor.log
    case actor: DiagnosticActorLogging => actor.log
    case _                             => system.log
  }

  private def clearAndReplay(timeout: FiniteDuration) = {
    val persistenceId = actor.persistenceId

    val deleteEvents = {
      val futureRef = FutureRef(system, timeout)
      actor.journal.tell(DeleteMessagesTo(persistenceId, Int.MaxValue, futureRef.ref), futureRef.ref)
      futureRef.future map {
        case x: DeleteMessagesSuccess => x
        case x: DeleteMessagesFailure => x
      }
    }

    val deleteSnapshots = {
      val futureRef = FutureRef(system, timeout)
      actor.snapshotStore.tell(DeleteSnapshots(persistenceId, SnapshotSelectionCriteria()), futureRef.ref)
      futureRef.future map {
        case x: DeleteSnapshotsSuccess => x
        case x: DeleteSnapshotsFailure => x
      }
    }

    val delete = for {
      _ <- deleteEvents
      _ <- deleteSnapshots
    } yield ()

    Await.result(delete, timeout)

    val stashSupport = ReflectValue[StashSupport]("Eventsourced$$internalStash", actor)
    val stash = stashSupport.clearStash()
    system.scheduler.scheduleOnce(replayDelay) {
      replay(stash)
    }
  }
}

object RecoveryBreaker {

  def apply(
    saveSnapshotOncePer: Long,
    allowedNumberOfEvents: Long,
    actor: PersistentActor,
    action: => RecoveryBreaker.Action,
    replayDelay: FiniteDuration = 300.millis)(replay: Iterable[Envelope] => Any): RecoveryBreaker = {

    new RecoveryBreakerImp(
      saveSnapshotOncePer = saveSnapshotOncePer,
      allowedNumberOfEvents = allowedNumberOfEvents,
      actor = actor,
      action = action,
      replay = replay,
      replayDelay = replayDelay)
  }

  sealed trait Action
  object Action {
    case object Ignore extends Action
    case object Stop extends Action
    case class Clear(timeout: FiniteDuration = 30.seconds) extends Action
  }

  class BreakRecoveryException(msg: String) extends RuntimeException(msg) with NoStackTrace

  object ReflectValue {
    def apply[T](name: String, instance: AnyRef): T = {
      val fields = instance.getClass.getDeclaredFields
      val field = fields find {_.getName endsWith name} getOrElse {throw new NoSuchFieldException(name)}
      field.setAccessible(true)
      val value = field.get(instance)
      value.asInstanceOf[T]
    }
  }
}
