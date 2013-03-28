package com.signalcollect.triplerush

import com.signalcollect._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

object SignalSet extends Enumeration with Serializable {
  val BoundSubject = Value
  val BoundPredicate = Value
  val BoundObject = Value
}

class IndexVertex(id: TriplePattern) extends PatternVertex(id) {

  override def edgeCount = targetIds.length

  var targetIds: ArrayBuffer[TriplePattern] = new ArrayBuffer(1)

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val targetId = e.targetId.asInstanceOf[TriplePattern]
    targetIds += targetId
    true
  }

  def processSamplingQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    throw new Exception("Sampling currently not supported")
    //TODO: Fix for triple vertex style interactions
    //    val targetIdCount = targetIds.length
    //    val bins = new Array[Long](targetIdCount)
    //    var binIndex = 0
    //    while (binIndex < query.tickets) {
    //      val index = Random.nextInt(targetIdCount)
    //      bins(index) += 1
    //      binIndex += 1
    //    }
    //    val complete: Boolean = bins forall (_ > 0)
    //    var targetIdIndex = 0
    //    while (targetIdIndex < targetIdCount) {
    //      val ticketsForEdge = bins(targetIdIndex)
    //      if (ticketsForEdge > 0) {
    //        val ticketEquippedQuery = query.withTickets(ticketsForEdge, complete)
    //        val targetId = targetIds(targetIdIndex)
    //        routeQuery(ticketEquippedQuery, targetId, graphEditor)
    //      }
    //      targetIdIndex += 1
    //    }
  }

  def processFullQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val targetIdCount = targetIds.length
    val avg = query.tickets / targetIdCount
    val complete = avg > 0
    var extras = new AtomicLong(query.tickets % targetIdCount)
    val averageTicketQuery = query.withTickets(avg, complete)
    val aboveAverageTicketQuery = query.withTickets(avg + 1, complete)
    for (targetId <- targetIds) {
      val hasExtra = extras.decrementAndGet >= 0
      if (hasExtra) {
        graphEditor.sendSignal(aboveAverageTicketQuery, targetId, None)
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