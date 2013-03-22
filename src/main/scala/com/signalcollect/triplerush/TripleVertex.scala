package com.signalcollect.triplerush

import com.signalcollect._

class TripleVertex(override val id: TriplePattern, initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }

  type Signal = PatternQuery
  def collect(signal: PatternQuery) = {
    signal :: state
  }

  override def scoreSignal = state.size

  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    val boundQueries = state flatMap (_.bind(id))
    val (fullyMatched, partiallyMatched) = boundQueries partition (_.unmatched.isEmpty)

    fullyMatched foreach { query =>
      println(s"Sending ${query.tickets} to the query vertex")
      graphEditor.sendSignal(query, query.queryId, None)
    }
    partiallyMatched foreach (query => {
      //      if (query.matched.head.s.value == Mapping.getId("http://www.Department0.University0.edu/AssistantProfessor0")) {
      //        println("next destination: " + query.nextTargetId +
      //            "\npassenger=" + query.matched.head + 
      //            "\ncurrently @ " + id + 
      //            "\nfull query = " + query)
      //      }
      graphEditor.sendSignal(query, query.nextTargetId.get, None)
    })
    state = List()
  }
}