package com.signalcollect.triplerush.loading

import java.io.File
import org.scalatest.{ Finders, FlatSpec }
import org.scalatest.concurrent.ScalaFutures
import com.signalcollect.triplerush.{ TriplePattern, TripleRush }
import com.signalcollect.triplerush.dictionary.HashDictionary
import org.apache.jena.riot.Lang
import com.signalcollect.triplerush.GroundTruthSpec

class BlockingAdditionsSpec extends FlatSpec with ScalaFutures {

  "Blocking additions" should "correctly load triples from a file" in {
    //fastStart = true, 
    val tr = TripleRush()
    try {
      println("Loading LUBM1 ... ")
      val resource = s"university0_0.nt"
      val tripleStream = classOf[GroundTruthSpec].getResourceAsStream(resource)
      println(s"Loading file $resource ...")
      tr.addTriples(TripleIterator(tripleStream, Lang.NTRIPLES))
      println(s"Done loading $resource.")
      println("Finished loading LUBM1.")
      val howMany = 25700
      println(tr.dictionary)
      println(tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size)
      println(tr.countVerticesByType)
      println(tr.edgesPerIndexType)
    } finally {
      tr.shutdown
    }
  }

}
