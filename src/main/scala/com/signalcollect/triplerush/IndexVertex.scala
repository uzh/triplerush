package com.signalcollect.triplerush

import com.signalcollect._

object SignalSet extends Enumeration with Serializable {
  val BoundSubject = Value
  val BoundPredicate = Value
  val BoundObject = Value
}

class IndexVertex(override val id: TriplePattern, initialState: List[PatternQuery] = List())
  extends DataFlowVertex(id, initialState)
  with ResetStateAfterSignaling[TriplePattern, List[PatternQuery]] {

  def resetState = List()

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new QueryListEdge(id))
    }
  }

  val signalSet: SignalSet.Value = {
    if (id.s.isWildcard) {
      SignalSet.BoundSubject
    } else if (id.p.isWildcard) {
      SignalSet.BoundPredicate
    } else {
      SignalSet.BoundObject
    }
  }

  val fractionDivider = 1 // TODO this is the number we cannot determine with this approach. edgeCount.toDouble / id.splitFactor

  type Signal = List[PatternQuery]
  def collect(signal: List[PatternQuery]) = {
    (signal map (_.split(fractionDivider))) ::: state
  }

}