package com.signalcollect.triplerush

import com.signalcollect._
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

class QueryVertex(
  val id: Int,
  val promise: Promise[(List[PatternQuery], Map[String, Any])],
  val expectedTickets: Long,
  var state: List[PatternQuery] = List()) extends Vertex[Int, List[PatternQuery]] {
  var receivedTickets: Long = 0
  var firstResultNanoTime = 0l
  var complete = true
  def setState(s: List[PatternQuery]) {
    state = s
  }
  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val query = signal.asInstanceOf[PatternQuery]
    receivedTickets += query.tickets
    complete &&= query.isComplete
//    println(s"queryId: $id tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
    if (!query.isFailed) {
      if (firstResultNanoTime == 0) {
        firstResultNanoTime = System.nanoTime
      }
      state = query :: state
    }
    true
  }
  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    //println(s"$id Result will be submitted. This query is going to do its laundry. Tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
    promise success (state, Map("firstResultNanoTime" -> firstResultNanoTime))
    graphEditor.removeVertex(id)
    //println(s"Query $id returns result: is complete = $complete" )
  }
  override def scoreSignal: Double = if (expectedTickets == receivedTickets) 1 else 0
  def scoreCollect = 0 // Because signals are collected upon delivery.
  def edgeCount = 0
  override def toString = s"${this.getClass.getName}(state=$state)"
  def afterInitialization(graphEditor: GraphEditor[Any, Any]) {}
  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {}
  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}
  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = 0
}