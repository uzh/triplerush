package com.signalcollect.pathqueries

import com.signalcollect.GraphEditor
import com.signalcollect.DataFlowVertex
import com.signalcollect.StateForwarderEdge

class TripleVertex(override val id: (Int, Int, Int), initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    val xpoId = (0, id._2, id._3)
    val sxoId = (id._1, 0, id._3)
    val spxId = (id._1, id._2, 0)
    graphEditor.addVertex(new IndexVertex(xpoId))
    graphEditor.addVertex(new IndexVertex(sxoId))
    graphEditor.addVertex(new IndexVertex(spxId))
    graphEditor.addEdge(xpoId, new StateForwarderEdge(id))
    graphEditor.addEdge(sxoId, new StateForwarderEdge(id))
    graphEditor.addEdge(spxId, new StateForwarderEdge(id))
  }

  type Signal = List[PatternQuery]
  def collect(signal: List[PatternQuery]) = signal ::: state
  override def scoreSignal = state.size

  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    val boundQueries = state flatMap (_.bindTriple(id._1, id._2, id._3))
    val (fullyMatched, partiallyMatched) = boundQueries partition (_.unmatched.isEmpty)
    
    fullyMatched foreach { query =>
      graphEditor.sendSignal(query, query.queryId, None)
      //query => println("Solution: " + query.bindings.toString))
    }
    partiallyMatched foreach (query => {
      graphEditor.sendSignal(List(query), query.nextTargetId.get, None)
    })
    state = List()
  }
}