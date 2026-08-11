package com.evolutiongaming.persistence

import org.scalatest.matchers.should.Matchers._
import org.scalatest.OptionValues._
import org.scalatest.wordspec.AnyWordSpec

class PersistenceIdSpec extends AnyWordSpec {

  "PersistenceId.unapply" should {
    "be empty for invalid string" in {
      PersistenceId.unapply("") shouldBe empty
      PersistenceId.unapply("AAA") shouldBe empty
      PersistenceId.unapply("AAA BBB") shouldBe empty
    }

    "be tuple for valid string" in {
      PersistenceId.unapply("aaa-bbb").value shouldBe (("aaa", "bbb"))
      PersistenceId.unapply("aaa-").value shouldBe (("aaa", ""))
      PersistenceId.unapply("-bbb").value shouldBe (("", "bbb"))
      PersistenceId.unapply("-").value shouldBe (("", ""))
    }
  }

}
