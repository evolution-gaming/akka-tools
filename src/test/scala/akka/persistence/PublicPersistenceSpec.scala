package akka.persistence

import com.evolutiongaming.util.ActorSpec
import org.scalatest.{Matchers, WordSpec}

class PublicPersistenceSpec extends WordSpec with ActorSpec with Matchers {
  "PublicPersistence" when {
    "journalFor" should {
      "return" in new ActorScope {
        PublicPersistence(system).journalFor("akka.persistence.journal.inmem") should not be null
      }
    }
    "snapshotStoreFor" should {
      "return" in new ActorScope {
        PublicPersistence(system).snapshotStoreFor("akka.persistence.snapshot-store.local") should not be null
      }
    }
  }
}
