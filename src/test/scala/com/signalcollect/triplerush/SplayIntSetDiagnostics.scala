package com.signalcollect.triplerush

import com.signalcollect.triplerush.sparql.Sparql
import com.signalcollect.triplerush.vertices.OptimizedIndexVertex
import com.signalcollect.util.FastInsertIntSet
import com.signalcollect.util.SplayIntSet
import com.signalcollect.util.FastInsertIntSet

object SplayIntSetDiagnostics extends App {

  implicit val tr = new TripleRush
  Lubm.load(tr)

  tr.graph.foreachVertex {
    v =>
      v match {
        case i: OptimizedIndexVertex =>
          val size = i.numberOfStoredChildDeltas
          if (i.state != null) {
            i.state match {
              case i: Int =>
              //println(s"${new EfficientIndexPattern(v.id).toTriplePattern} only stores child delta $i.")
              case a: Array[Byte] =>
                val f = new FastInsertIntSet(a)
                val min = f.min
                val max = f.max
                val range = f.max - f.min
                val density = ((f.size / range.toDouble) * 1000).round / 10.0
                if (density > 10.0 && size > 10) {
                  println(s"${new EfficientIndexPattern(v.id).toTriplePattern} has $size stored child deltas inside of one FastInsertIntSet.")
                  println(s"Ids range between ${min} and ${max}: $range")
                  println(s"Density is $density%")
                }
              case s: SplayIntSet =>
                println(s"${new EfficientIndexPattern(v.id).toTriplePattern} has $size stored child deltas inside of a SplayIntSet:")
                s.printDiagnosticInfo
            }
          }
        case other =>
      }
  }

  tr.shutdown
}
