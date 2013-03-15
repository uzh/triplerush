package com.signalcollect.triplerush

import com.signalcollect.GraphEditor
import com.signalcollect.DataFlowVertex
import com.signalcollect.StateForwarderEdge

class IndexVertex(override val id: TriplePattern, initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }

  type Signal = List[PatternQuery]
  def collect(signal: List[PatternQuery]) = {
    (signal map (_.split(edgeCount))) ::: state
  }

  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    state foreach (query => {
      outgoingEdges.values.foreach { edge =>
        //println(s"id $id is sending ${query.unmatched.head} to ${edge.targetId}")
        graphEditor.sendSignal(List(query), edge.targetId, None)
      }
    })
    state = List()
  }

  // TODO: Reset state after signalling.

}