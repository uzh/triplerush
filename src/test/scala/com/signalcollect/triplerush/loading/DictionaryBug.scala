package com.signalcollect.triplerush.loading

import java.io.File
import org.scalatest.{ Finders, FlatSpec }
import org.scalatest.concurrent.ScalaFutures
import com.signalcollect.triplerush.{ TriplePattern, TripleRush }
import com.signalcollect.triplerush.dictionary.HashDictionary
import org.apache.jena.riot.Lang
import com.signalcollect.triplerush.GroundTruthSpec
import com.signalcollect.triplerush.TestStore

object DictionaryBug extends App {

  val d = new HashDictionary
  try {
    val howMany = 1000000
    val strings = (1 to howMany).map(i => ((i / 100) % 3).toString)
    val ids = strings.par.map(d(_))
    println(ids.toSet.size)
  } finally {
    d.close()
  }

}
