package akka.persistence

import com.evolutiongaming.test.ActorSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PublicPersistenceSpec extends AnyWordSpec with ActorSpec with Matchers {
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
