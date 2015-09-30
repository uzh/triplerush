package com.signalcollect.triplerush.loading

import java.io.File
import org.scalatest.{ Finders, FlatSpec }
import org.scalatest.concurrent.ScalaFutures
import com.signalcollect.triplerush.{ TriplePattern, TripleRush }
import com.signalcollect.triplerush.dictionary.HashDictionary
import com.signalcollect.util.TestAnnouncements
import com.signalcollect.triplerush.vertices.IndexVertex
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.EfficientIndexPattern._

object Issue34 extends App {

  //fastStart = true, 
  val tr = TripleRush()
  //val tr = TripleRush(dictionary = new ModularDictionary())
  try {
    //    val filePath = s".${File.separator}lubm${File.separator}university0_0.nt"
    //    println(s"Loading file $filePath ...")
    //tr.loadFromFile(filePath)
    val howMany = 25700
    //.take(howMany)
    tr.prepareExecution
    //    tr.addTriplePattern(TriplePattern(1, 2, 3), blocking = true)
    //    println("From here on out all should be the same")
    val sillyIterator = new Iterator[TriplePattern] {
      var i = 1
      def next = {
        val n = i
        i += 1
        TriplePattern(1, n, 1)
      }
      def hasNext = i <= howMany
    }
    //    tr.addTriplePatterns(sillyIterator, blocking = true)
    //    tr.awaitIdle()
    //tr.addTriplePattern(TriplePattern(1, 2, 3), blocking = true)
    //tr.addTriplePattern(TriplePattern(4, 5, 6), blocking = true)
    //tr.addTriplePattern(TriplePattern(1, 2, 3), blocking = true)
    //    tr.awaitIdle

    val filePath = s".${File.separator}lubm${File.separator}university0_0.nt"
    println(s"Loading file $filePath ...")
    //tr.loadFromFile(filePath)
    //.take(howMany)
    tr.addTriples(TripleIterator(filePath), blocking = true)

    println(tr.dictionary)
    //tr.awaitIdle()
    //val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
    println(tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size)
    //assert(count == howMany)
    println(tr.countVerticesByType)
    println(tr.edgesPerIndexType)
    var i = 0
    tr.graph.foreachVertex { v =>
      i += 1
    }
    println(tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size)
    //    tr.graph.foreachVertex { v =>
    //      val count = v.edgeCount
    //      if (count != 1 && count != howMany) {
    //        var deltas = Set.empty[Int]
    //        v.asInstanceOf[IndexVertex[Any]].foreachChildDelta { d => deltas += d }
    //        if (deltas.size < 10) {
    //          println(s"vertex ${v.id.asInstanceOf[Long].toTriplePattern} has edge count ${v.edgeCount} with deltas $deltas")
    //        } else {
    //          val missing = (1 to howMany).toSet -- deltas
    //          println(s"vertex ${v.id.asInstanceOf[Long].toTriplePattern} has edge count ${v.edgeCount} with missing deltas $missing")
    //        }
    //      }
    //    }
  } finally {
    tr.shutdown
  }

}
