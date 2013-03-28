package com.signalcollect.triplerush

import com.signalcollect._
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

class QueryVertex(
  val id: Int,
  val promise: Promise[ArrayBuffer[PatternQuery]],
  val expectedTickets: Long,
  var state: ArrayBuffer[PatternQuery] = new ArrayBuffer(180000)) extends Vertex[Int, ArrayBuffer[PatternQuery]] {
  var receivedTickets: Long = 0
  var complete = true
  def setState(s: ArrayBuffer[PatternQuery]) {
    state = s
  }
  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val query = signal.asInstanceOf[PatternQuery]
    receivedTickets += query.tickets
    complete &&= query.isComplete
//    println(s"$id tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
    if (!query.isFailed) {
      state.append(query)
    }
    true
  }
  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    //println(s"$id Result will be submitted. This query is going to do its laundry. Tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
    promise success state
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
  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException("Use setTargetIds(...)")
  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = 0
}
//    complete &&= query.isComplete
//    println(s"$id tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
//    if (!query.isFailed) {
//      query :: state
//    } else {
//      state
//    }
//  }
//  override def scoreSignal = if (expectedTickets == receivedTickets) 1 else 0
//  
//  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
//    //println("Query added: " + id)
//  }
//
//  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
//    //println(s"$id Result will be submitted. This query is going to do its laundry. Tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
//    promise success state
//    graphEditor.removeVertex(id)
//    //println(s"Query $id returns result: is complete = $complete" )
//  }
//}