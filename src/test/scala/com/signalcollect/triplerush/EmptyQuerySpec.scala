package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import org.scalacheck.Gen._
import com.signalcollect.triplerush.optimizers.CleverCardinalityOptimizer

class EmptyQuerySpec extends FlatSpec with Matchers with TestAnnouncements {

  "TripleRush" should "correctly answer result counts for queries with zero results" in {
    val tr = new TripleRush
    try {
      tr.prepareExecution
      val query = Seq(TriplePattern(-1, 2, 3))
      val result = tr.executeQuery(query, Some(CleverCardinalityOptimizer))
    } finally {
      tr.shutdown
    }
  }

}
