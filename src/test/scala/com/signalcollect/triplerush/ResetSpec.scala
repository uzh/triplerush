package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import org.scalacheck.Gen._
import com.signalcollect.util.TestAnnouncements

class ResetSpec extends FlatSpec with Matchers with TestAnnouncements {

  "TripleRush" should "correctly function even after a reset" in new TestStore {
    tr.addTriplePattern(TriplePattern(1, 2, 3))
        val query = Seq(TriplePattern(-1, 2, 3))
    val result1 = tr.resultIteratorForQuery(query)
    tr.clear
    tr.addTriplePattern(TriplePattern(3, 2, 3))
        val result2 = tr.resultIteratorForQuery(query)
  }

}
