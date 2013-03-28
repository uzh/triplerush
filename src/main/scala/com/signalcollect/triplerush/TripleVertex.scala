package com.signalcollect.triplerush

import com.signalcollect._
import scala.collection.mutable.ArrayBuffer

class TripleVertex(val id: TriplePattern, var state: ArrayBuffer[PatternQuery] = new ArrayBuffer(1))
  extends Vertex[TriplePattern, ArrayBuffer[PatternQuery]] {
  def setState(s: ArrayBuffer[PatternQuery]) {
    state = s
  }
  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val query = signal.asInstanceOf[PatternQuery]
    state.append(query)
    true
  }
  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    var i = 0
    while (i < state.length) {
      val query = state(i)
      val boundQueryOption = query.bind(id)
      if (boundQueryOption.isDefined) {
        val boundQuery = boundQueryOption.get
        if (boundQuery.unmatched.isEmpty) {
          graphEditor.sendSignal(boundQuery, boundQuery.queryId, None)
        } else {
          graphEditor.sendSignal(boundQuery, boundQuery.unmatched.head.routingAddress, None)
        }
      }
      i += 1
    }
    state.clear
  }
  def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }
  override def scoreSignal: Double = if (state.isEmpty) 0 else 1
  def scoreCollect = 0 // Because signals are collected upon delivery.
  def edgeCount = 0
  override def toString = s"${this.getClass.getName}(state=$state)"
  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {}
  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}
  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException("Use setTargetIds(...)")
  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = 0
}
