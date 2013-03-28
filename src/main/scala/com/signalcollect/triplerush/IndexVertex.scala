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

class IndexVertex(override val id: TriplePattern)
  extends Vertex[TriplePattern, ArrayBuffer[PatternQuery]] {

  var state = new ArrayBuffer[PatternQuery](1)

  def setState(s: ArrayBuffer[PatternQuery]) {
    state = s
  }

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val targetId = e.targetId.asInstanceOf[TriplePattern]
    targetIds += targetId
    true
  }

  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val query = signal.asInstanceOf[PatternQuery]
    state.append(query)
    true
  }

  def processSamplingQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
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
    if (id.isFullyBound) {
      // This is a triple vertex.
      bindAndRouteQuery(query, id, graphEditor)
    } else {
      // This is an index vertex.
      val targetIdCount = targetIds.length
      val avg = query.tickets / targetIdCount
      val complete = avg > 0
      var extras = new AtomicLong(query.tickets % targetIdCount)
      val averageTicketQuery = query.withTickets(avg, complete)
      val aboveAverageTicketQuery = query.withTickets(avg + 1, complete)
//      val targetIdArray = {
//        if (targetIdCount > 100) {
//          targetIds.par
//        } else {
//          targetIds
//        }
//      }
      for (targetId <- targetIds) {
        val ticketEquippedQuery = {
          val hasExtra = extras.decrementAndGet >= 0
          if (hasExtra) {
            aboveAverageTicketQuery
          } else {
            averageTicketQuery
          }
        }
        //        if (!targetId.isFullyBound) {
        signal(ticketEquippedQuery, targetId, graphEditor)
        //        } else {
        //          bindAndRouteQuery(ticketEquippedQuery, targetId, graphEditor)
        //        }
      }
    }
  }

  /**
   * Binds 'query' to 'pattern' and routes the query to the next destination.
   */
  def bindAndRouteQuery(query: PatternQuery, pattern: TriplePattern, graphEditor: GraphEditor[Any, Any]) {
    val boundQueryOption = query.bind(pattern)
    if (!boundQueryOption.isDefined) {
      signal(query.failed, query.queryId, graphEditor)
    } else {
      val boundQuery = boundQueryOption.get
      if (boundQuery.unmatched.isEmpty) {
        signal(boundQuery, boundQuery.queryId, graphEditor)
      } else {
        val routingAddress = boundQuery.unmatched.head.routingAddress
        //        println(s"Routing into index: routeQuery=$boundQuery targetId=${boundQuery.unmatched.head.routingAddress}")
        signal(boundQuery, routingAddress, graphEditor)
      }
    }
  }

  def signal(query: PatternQuery, targetId: Any, graphEditor: GraphEditor[Any, Any]) {
//    synchronized {
      graphEditor.sendSignal(query, targetId, None)
//    }
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    for (query <- state) {
      if (query.isSamplingQuery) {
        processSamplingQuery(query, graphEditor)
      } else {
        processFullQuery(query, graphEditor)
      }
    }
    state.clear
  }

  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {
  }

  override def scoreSignal: Double = state.size

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = targetIds.length

  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}

  def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }

  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = throw new UnsupportedOperationException

  var targetIds: ArrayBuffer[TriplePattern] = new ArrayBuffer(0)
  // TODO: if (id.isFullyBound) null.asInstanceOf[ArrayBuffer[TriplePattern]] else new ArrayBuffer(1)

}