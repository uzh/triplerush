package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import org.scalacheck.Gen._
import com.signalcollect.triplerush.optimizers.CleverCardinalityOptimizer

class ResetSpec extends FlatSpec with Matchers with TestAnnouncements {

  "TripleRush" should "correctly function even after a reset" in {
    val tr = new TripleRush
    try {
      tr.addTriplePattern(TriplePattern(1, 2, 3))
      tr.prepareExecution
      val query = Seq(TriplePattern(-1, 2, 3))
      val result1 = tr.executeQuery(query, Some(CleverCardinalityOptimizer))
      tr.clear
      tr.addTriplePattern(TriplePattern(3, 2, 3))
      tr.prepareExecution
      val result2 = tr.executeQuery(query, Some(CleverCardinalityOptimizer))
    } finally {
      tr.shutdown
    }
  }

}
