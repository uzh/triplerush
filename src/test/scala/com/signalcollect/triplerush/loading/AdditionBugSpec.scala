package com.signalcollect.triplerush.loading

import java.io.File
import org.scalatest.{ Finders, FlatSpec }
import org.scalatest.concurrent.ScalaFutures
import com.signalcollect.triplerush.{ TriplePattern, TripleRush }
import com.signalcollect.triplerush.dictionary.HashDictionary
import org.apache.jena.riot.Lang
import com.signalcollect.triplerush.GroundTruthSpec
import com.signalcollect.triplerush.TestStore

object AdditionBugSpec extends App {

  val tr = TestStore.instantiateUniqueStore()
  try {
    val howMany = 30000
//    val triplePatterns = (1 to howMany).map(i => TriplePattern(1, i, 1))
//    tr.addTriplePatterns(triplePatterns.iterator)
    val triplePatterns = (1 to howMany).map(i => ("1", i.toString, "1"))
    tr.addStringTriples(triplePatterns.iterator)
    println("Finished loading")
    println(tr.dictionary)
    println(tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size)
  } finally {
    tr.shutdown
    tr.graph.shutdown()
    tr.graph.system.shutdown()
  }

}
