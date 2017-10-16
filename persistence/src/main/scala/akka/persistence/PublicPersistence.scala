package akka.persistence

import akka.actor.{ActorRef, ActorSystem}

class PublicPersistence(persistence: Persistence) {

  def journalFor(journalPluginId: String): ActorRef = {
    persistence.journalFor(journalPluginId)
  }

  def snapshotStoreFor(snapshotPluginId: String): ActorRef = {
    persistence.snapshotStoreFor(snapshotPluginId)
  }
}

object PublicPersistence {
  def apply(persistence: Persistence): PublicPersistence = new PublicPersistence(persistence)
  def apply(system: ActorSystem): PublicPersistence = new PublicPersistence(Persistence(system))
}
