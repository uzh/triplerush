package com.signalcollect.triplerush

import com.signalcollect.triplerush.sparql.Sparql
import com.signalcollect.triplerush.vertices.OptimizedIndexVertex
import com.signalcollect.util.FastInsertIntSet
import com.signalcollect.util.SplayIntSet

object SplayIntSetDiagnostics extends App {

  implicit val tr = new TripleRush
  Lubm.load(tr)

  tr.graph.foreachVertex {
    v =>
      v match {
        case i: OptimizedIndexVertex =>
          val size = i.numberOfStoredChildDeltas
          i.state match {
            case a: Array[Byte] =>
              println(s"${new EfficientIndexPattern(v.id).toTriplePattern} has $size stored child deltas inside of one FastInsertIntSet.")
            case s: SplayIntSet =>
              println(s"${new EfficientIndexPattern(v.id).toTriplePattern} has $size stored child deltas inside of a SplayIntSet:")
              s.printDiagnosticInfo
          }
        case other =>
      }
  }

  tr.shutdown
}
