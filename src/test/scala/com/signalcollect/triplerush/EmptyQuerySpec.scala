package com.signalcollect.triplerush

import org.scalatest.fixture.{ FlatSpec, UnitFixture }
import org.scalatest.Matchers
import org.scalacheck.Arbitrary
import org.scalatest.prop.Checkers
import org.scalacheck.Gen._

class EmptyQuerySpec extends FlatSpec with UnitFixture with Matchers {

  "TripleRush" should "correctly answer result counts for queries with zero results" in new TestStore {
    val query = Seq(TriplePattern(-1, 2, 3))
    val result = tr.resultIteratorForQuery(query)
    assert(result.size == 0)
  }

}
