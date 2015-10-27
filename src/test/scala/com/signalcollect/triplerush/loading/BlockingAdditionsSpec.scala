package com.signalcollect.triplerush.loading

import org.apache.jena.riot.Lang
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures
import com.signalcollect.triplerush.{ GroundTruthSpec, TestStore, TriplePattern }
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class BlockingAdditionsSpec extends FlatSpec with ScalaFutures {

  "Blocking additions" should "correctly load triples from a file" in  {
    val tr = TestStore.instantiateUniqueStore()
    try {
      val resource = s"university0_0.nt"
      val tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)
      println(s"Loading file $resource ...")
      tr.addTriples(TripleIterator(tripleStream, Lang.NTRIPLES))
      println(s"Done loading $resource.")
      val expectedCount = 25700
      val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
      assert(count == expectedCount)
    } finally {
      tr.shutdown
      Await.result(tr.graph.system.terminate(), Duration.Inf)
    }
  }
}
