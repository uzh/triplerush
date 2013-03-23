package com.signalcollect.triplerush

import com.signalcollect._
import scala.concurrent.Promise

class QueryVertex(id: Int, promise: Promise[List[PatternQuery]], val expectedTickets: Long, initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  type Signal = PatternQuery
  var receivedTickets: Long = 0
  var complete = true
  
  def collect(query: PatternQuery) = {
    receivedTickets += query.tickets
    complete &&= query.isComplete 
    //println(s"$id tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
    if (!query.isFailed) {
      query :: state
    } else {
      state
    }
  }
  override def scoreSignal = if (expectedTickets == receivedTickets) 1 else 0
  
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    //println("Query added: " + id)
  }

  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    //println(s"$id Result will be submitted. This query is going to do its laundry. Tickets: $receivedTickets/$expectedTickets. Total bindings so far: ${state.filter(!_.isFailed).length}.")
    promise success state
    graphEditor.removeVertex(id)
    //println(s"Query $id returns result: is complete = $complete" )
  }
}