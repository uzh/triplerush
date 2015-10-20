package com.signalcollect.triplerush.loading

import org.apache.jena.riot.Lang
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures

import com.signalcollect.triplerush.{ GroundTruthSpec, TestStore, TriplePattern }

class BlockingAdditionsSpec extends FlatSpec with ScalaFutures {

  "Blocking additions" should "correctly load triples from a file" in new TestStore {
    val resource = "university0_0.nt"
    val tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)
    tr.addTriples(TripleIterator(tripleStream, Lang.NTRIPLES))
    val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
    val expectedCount = 25700
    assert(count == expectedCount)
  }

}
