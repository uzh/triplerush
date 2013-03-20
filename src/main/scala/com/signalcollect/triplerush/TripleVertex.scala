package com.signalcollect.triplerush

import com.signalcollect.GraphEditor
import com.signalcollect.DataFlowVertex
import com.signalcollect.StateForwarderEdge

class TripleVertex(override val id: TriplePattern, initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new QueryListEdge(id))
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
//      if (query.matched.head.s.value == Mapping.getId("http://www.Department0.University0.edu/AssistantProfessor0")) {
//        println("next destination: " + query.nextTargetId +
//            "\npassenger=" + query.matched.head + 
//            "\ncurrently @ " + id + 
//            "\nfull query = " + query)
//      }
      graphEditor.sendSignal(List(query), query.nextTargetId.get, None)
    })
    state = List()
  }
}