package com.signalcollect.pathqueries

import com.signalcollect.GraphEditor
import com.signalcollect.DataFlowVertex
import com.signalcollect.StateForwarderEdge

class IndexVertex(override val id: (Int, Int, Int), initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id match {
      case (0, 0, 0) =>
      case (s, 0, 0) =>
        val xxxId = (0, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxxId))
        graphEditor.addEdge(xxxId, new StateForwarderEdge(id))
      case (0, p, 0) =>
        val xxxId = (0, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxxId))
        graphEditor.addEdge(xxxId, new StateForwarderEdge(id))
      case (0, 0, o) =>
        val xxxId = (0, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxxId))
        graphEditor.addEdge(xxxId, new StateForwarderEdge(id))
      case (0, p, o) =>
        val xxoId = (0, 0, id._3)
        val xpxId = (0, id._2, 0)
        graphEditor.addVertex(new IndexVertex(xxoId))
        graphEditor.addVertex(new IndexVertex(xpxId))
        graphEditor.addEdge(xxoId, new StateForwarderEdge(id))
        graphEditor.addEdge(xpxId, new StateForwarderEdge(id))
      case (s, 0, o) =>
        val xxoId = (0, 0, id._3)
        val sxxId = (id._1, 0, 0)
        graphEditor.addVertex(new IndexVertex(xxoId))
        graphEditor.addVertex(new IndexVertex(sxxId))
        graphEditor.addEdge(xxoId, new StateForwarderEdge(id))
        graphEditor.addEdge(sxxId, new StateForwarderEdge(id))
      case (s, p, 0) =>
        val xpxId = (0, id._2, 0)
        val sxxId = (id._1, 0, 0)
        graphEditor.addVertex(new IndexVertex(xpxId))
        graphEditor.addVertex(new IndexVertex(sxxId))
        graphEditor.addEdge(xpxId, new StateForwarderEdge(id))
        graphEditor.addEdge(sxxId, new StateForwarderEdge(id))
      case other => println("Everything defined, this cannot be an index vertex: " + id)
    }
  }

  type Signal = List[PatternQuery]
  def collect(signal: List[PatternQuery]) = {
    (signal map (_.split(edgeCount))) ::: state
  }
  
  // TODO: Reset state after signalling.
  
}