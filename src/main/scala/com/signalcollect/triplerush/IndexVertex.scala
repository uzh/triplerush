package com.signalcollect.triplerush

import com.signalcollect._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SignalSet extends Enumeration with Serializable {
  val BoundSubject = Value
  val BoundPredicate = Value
  val BoundObject = Value
}

class IndexVertex(override val id: TriplePattern)
  extends Vertex[TriplePattern, List[PatternQuery]] {

  var state = List[PatternQuery]()
  def outEdges = activeSet.length

  def setState(s: List[PatternQuery]) {
    state = s
  }

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val targetId = e.targetId.asInstanceOf[TriplePattern]
    if (id.s.isWildcard && targetId.isPartOfSignalSet(SignalSet.BoundSubject)) {
      subjectSet += targetId
    } else if (id.p.isWildcard && targetId.isPartOfSignalSet(SignalSet.BoundPredicate)) {
      predicateSet += targetId
    } else if (id.o.isWildcard && targetId.isPartOfSignalSet(SignalSet.BoundObject)) {
      objectSet += targetId
    } else {
      throw new Exception(s"Cannot add edge $e to index vertex with id $id.")
    }
    true
  }

  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val s = signal.asInstanceOf[PatternQuery]
    setState(s :: state)
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    val edgeSet = activeSet
    val edgeSetLength = edgeSet.length
    state foreach (query => {
      if (query.isSamplingQuery) {
        println("Executed as sampling query")
        val bins = new Array[Long](edgeSetLength)
        var i = 0
        while (i < query.tickets) {
          val index = Random.nextInt(edgeSetLength)
          bins(index) += 1
          i += 1
        }
        val complete: Boolean = bins forall (_ > 0)
        println(id + " " + bins.toList)
        i = 0
        while (i < edgeSetLength) {
          val ticketsForEdge = bins(i)
          if (ticketsForEdge > 0) {
            graphEditor.sendSignal(query.withTickets(ticketsForEdge, complete), edgeSet(i), None)
          }
          i += 1
        }
      } else {
        val avg = query.tickets / edgeSetLength
        val complete = avg > 0
        var extras = query.tickets % edgeSetLength
        val averageTicketQuery = query.withTickets(avg, complete)
        val aboveAverageTicketQuery = query.withTickets(avg + 1, complete)
        edgeSet foreach { targetId =>
          if (extras > 0) {
            graphEditor.sendSignal(aboveAverageTicketQuery, targetId, None)
            extras -= 1
          } else if (complete) {
            graphEditor.sendSignal(averageTicketQuery, targetId, None)
          }
        }
      }
    })
    setState(List())
  }

  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {
  }

  override def scoreSignal: Double = state.size

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = subjectSet.length + predicateSet.length + objectSet.length

  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}

  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = {
    throw new UnsupportedOperationException
  }

  def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }

  def activeSet = {
    // TODO: This is ugly, make it better.
    var minSize = Int.MaxValue
    var candidate = subjectSet
    if (id.s.isWildcard) {
      minSize = subjectSet.length
    }
    if (id.p.isWildcard && predicateSet.length < minSize) {
      candidate = predicateSet
      minSize = predicateSet.length
    }
    if (id.o.isWildcard && objectSet.length < minSize) {
      candidate = objectSet
    }
    candidate
  }

  var subjectSet: ArrayBuffer[TriplePattern] = ArrayBuffer()
  var predicateSet: ArrayBuffer[TriplePattern] = ArrayBuffer()
  var objectSet: ArrayBuffer[TriplePattern] = ArrayBuffer()

}