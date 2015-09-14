package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import org.scalacheck.Gen._
import com.signalcollect.util.TestAnnouncements

class EmptyQuerySpec extends FlatSpec with Matchers with TestAnnouncements {

  "TripleRush" should "correctly answer result counts for queries with zero results" in {
    val tr = TripleRush()
    try {
      tr.prepareExecution
      val query = Seq(TriplePattern(-1, 2, 3))
      val result = tr.resultIteratorForQuery(query)
      assert(result.size == 0)
    } finally {
      tr.shutdown
    }
  }

}
