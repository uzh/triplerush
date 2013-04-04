package com.signalcollect.triplerush

import com.signalcollect._
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

class QueryVertex(
  id: Int,
  val promise: Promise[(ArrayBuffer[PatternQuery], Map[String, Any])],
  val expectedTickets: Long) extends ProcessingVertex[Int, PatternQuery](id) {
  var receivedTickets: Long = 0
  var firstResultNanoTime = 0l
  var complete = true

  override def shouldProcess(query: PatternQuery): Boolean = {
    receivedTickets += query.tickets
    complete &&= query.isComplete
    if (!query.isFailed) {
      if (firstResultNanoTime == 0) {
        firstResultNanoTime = System.nanoTime
      }
      true
    } else {
      false
    }
  }

  override def scoreSignal: Double = if (expectedTickets == receivedTickets) 1 else 0

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    promise success (state, Map("firstResultNanoTime" -> firstResultNanoTime))
    graphEditor.removeVertex(id)
  }

  def process(item: PatternQuery, graphEditor: GraphEditor[Any, Any]) {}

}