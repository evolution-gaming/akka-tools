package com.evolutiongaming.util

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.persistence.{JournalFailure, PublicPersistence, Replicate, SnapshotFailure}

trait DuplicatingActor extends Actor with ActorLogging {

  val failureLogger = context actorOf FailureLogger.props
  val primary = actorFor(primaryName)
  val secondary = actorFor(secondaryName)
  require(primary != secondary)

  def actorForId(name: String): ActorRef

  def primaryName: String

  def secondaryName: String

  def receive: Receive = {
    case x =>
      primary.tell(x, sender())

      for {x <- Replicate.opt(x, failureLogger)} {
        secondary.tell(x, failureLogger)
      }
  }

  private def actorFor(name: String): ActorRef = {
    val config = context.system.settings.config
    val id = config getString name
    actorForId(id)
  }
}

object DuplicatingJournal {
  def props: Props = Props[DuplicatingJournal]
}

class DuplicatingJournal extends DuplicatingActor {
  def primaryName = "evolutiongaming.duplicating.journal.primary"

  def secondaryName = "evolutiongaming.duplicating.journal.secondary"

  def actorForId(id: String) = PublicPersistence(context.system).journalFor(id)
}

class DuplicatingSnapshotStore extends DuplicatingActor {
  def primaryName = "evolutiongaming.duplicating.snapshot-store.primary"

  def secondaryName = "evolutiongaming.duplicating.snapshot-store.secondary"

  def actorForId(id: String) = PublicPersistence(context.system).snapshotStoreFor(id)
}

class FailureLogger extends Actor with ActorLogging {
  def receive: Receive = {
    case JournalFailure(x)     => log.warning(s"Error received from ${ sender() }: $x")
    case SnapshotFailure(x)    => log.warning(s"Error received from ${ sender() }: $x")
    case Status.Failure(cause) => log.warning(s"Error received from ${ sender() }: ", cause)
  }
}

object FailureLogger {
  def props: Props = Props[FailureLogger]
}
