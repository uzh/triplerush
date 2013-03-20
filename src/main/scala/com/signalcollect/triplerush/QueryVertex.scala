package com.signalcollect.triplerush

import com.signalcollect.DataFlowVertex
import scala.concurrent.Promise
import com.signalcollect.GraphEditor

class QueryVertex(id: Int, promise: Promise[List[PatternQuery]], initialState: List[PatternQuery] = List()) extends DataFlowVertex(id, initialState) {
  type Signal = PatternQuery
  private var fractionCompleted = 0.0
  def collect(query: PatternQuery) = {
    fractionCompleted += query.fraction
    //println(s"$id completed fraction: $fractionCompleted")
    if (!query.isFailed) {
      query :: state
    } else {
      state
    }
  }
  override def scoreSignal = if (fractionCompleted > 0.999999) 1 else 0
  override def doSignal(graphEditor: GraphEditor[Any, Any]) {
    promise success state
    graphEditor.removeVertex(id)
    println("Query done: " + id)
  }
}