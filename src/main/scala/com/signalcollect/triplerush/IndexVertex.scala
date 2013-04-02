package com.signalcollect.triplerush

import com.signalcollect.Edge
import com.signalcollect.GraphEditor
import scala.util.Random

object SignalSet extends Enumeration with Serializable {
  val BoundSubject = Value
  val BoundPredicate = Value
  val BoundObject = Value
}

class IndexVertex(id: TriplePattern) extends PatternVertex(id) {

  override def edgeCount = targetIds.length

  var targetIds = List[TriplePattern]()

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val targetId = e.targetId.asInstanceOf[TriplePattern]
    targetIds = targetId :: targetIds
    true
  }

  def processSamplingQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val targetIdCount = targetIds.length
    val bins = new Array[Long](targetIdCount)
    for (i <- 1l to query.tickets) {
      val randomIndex = Random.nextInt(targetIdCount)
      bins(randomIndex) += 1
    }
    val complete: Boolean = bins forall (_ > 0)
    for (i <- 0 until targetIdCount) {
      val ticketsForEdge = bins(i)
      if (ticketsForEdge > 0) {
        val ticketEquippedQuery = query.withTickets(ticketsForEdge, complete)
        val targetId = targetIds(i)
        graphEditor.sendSignal(ticketEquippedQuery, targetId, None)
      }
    }
  }

  def processFullQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val targetIdCount = targetIds.length
    val avg = query.tickets / targetIdCount
    val complete = avg > 0
    var extras = query.tickets % targetIdCount
    val averageTicketQuery = query.withTickets(avg, complete)
    val aboveAverageTicketQuery = query.withTickets(avg + 1, complete)
    for (targetId <- targetIds) {
      if (extras > 0) {
        graphEditor.sendSignal(aboveAverageTicketQuery, targetId, None)
        extras -= 1
      } else if (complete) {
        graphEditor.sendSignal(averageTicketQuery, targetId, None)
      }
    }
  }

  override def signal(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    if (query.isSamplingQuery) {
      processSamplingQuery(query, graphEditor)
    } else {
      processFullQuery(query, graphEditor)
    }
  }
}