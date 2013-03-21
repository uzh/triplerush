package com.signalcollect.triplerush

import com.signalcollect._

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
      subjectSet = targetId :: subjectSet
    } else if (id.p.isWildcard && targetId.isPartOfSignalSet(SignalSet.BoundPredicate)) {
      predicateSet = targetId :: predicateSet
    } else if (id.o.isWildcard && targetId.isPartOfSignalSet(SignalSet.BoundObject)) {
      objectSet = targetId :: objectSet
    } else {
      // TODO: Remove this and throw exception.
      println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
    }
    true
  }

  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val s = signal.asInstanceOf[PatternQuery]
    setState(s :: state)
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    state foreach (query => {
      val splitQuery = query.split(activeSetLength)
      activeSet.foreach { targetId =>
        graphEditor.sendSignal(splitQuery, targetId, None)
      }
    })
    setState(List())
  }

  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {
  }

  override def scoreSignal: Double = state.size

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = activeSet.length

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

  def activeSetLength = activeSet.length

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

  var subjectSet: List[TriplePattern] = List()
  var predicateSet: List[TriplePattern] = List()
  var objectSet: List[TriplePattern] = List()

}