package com.signalcollect.triplerush

import com.signalcollect.GraphEditor
import com.signalcollect.DataFlowVertex
import com.signalcollect.StateForwarderEdge

class TripleVertex(override val id: TriplePattern, initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }

  type Signal = List[PatternQuery]
  def collect(signal: List[PatternQuery]) = {
    //println(signal.head.unmatched.head)
    signal ::: state
  }
  
  override def scoreSignal = state.size

  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    val boundQueries = state flatMap (_.bind(id))
    val (fullyMatched, partiallyMatched) = boundQueries partition (_.unmatched.isEmpty)

    fullyMatched foreach { query =>
      graphEditor.sendSignal(query, query.queryId, None)
    }
    partiallyMatched foreach (query => {
      graphEditor.sendSignal(List(query), query.nextTargetId.get, None)
    })
    state = List()
  }
}