package com.signalcollect.triplerush

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable
import org.scalatest.prop.Checkers
import org.scalacheck.Gen._
import com.signalcollect.triplerush.optimizers.CleverCardinalityOptimizer

class EmptyQuerySpec extends FlatSpec with Matchers with TestAnnouncements {

  it should "correctly answer result counts for queries with zero results" in {
    val tr = new TripleRush
    try {
      tr.prepareExecution
      val query = List(TriplePattern(-1, 2, 3))
      val result = tr.executeQuery(QuerySpecification(query), Some(CleverCardinalityOptimizer))
    } finally {
      tr.shutdown
    }
  }

}
