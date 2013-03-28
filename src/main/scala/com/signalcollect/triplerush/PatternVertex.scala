package com.signalcollect.triplerush

import com.signalcollect._
import scala.collection.mutable.ArrayBuffer

abstract class PatternVertex(
  val id: TriplePattern,
  var state: List[PatternQuery] = List.empty)
  extends Vertex[TriplePattern, List[PatternQuery]] {
  def setState(s: List[PatternQuery]) {
    state = s
  }

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }

  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val item = signal.asInstanceOf[PatternQuery]
    state = item :: state
    true
  }
  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    for (query <- state) {
      signal(query, graphEditor)
    }
    state = List.empty
  }

  def signal(query: PatternQuery, graphEditor: GraphEditor[Any, Any])

  override def scoreSignal: Double = if (state.isEmpty) 0 else 1
  def scoreCollect = 0 // Because signals are collected upon delivery.
  def edgeCount = 0
  override def toString = s"${this.getClass.getName}(state=$state)"
  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {}
  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}
  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException()
  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = 0
}
