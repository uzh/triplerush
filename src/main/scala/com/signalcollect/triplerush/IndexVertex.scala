/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.triplerush

import com.signalcollect.Edge
import com.signalcollect.GraphEditor
import com.signalcollect.examples.CompactIntSet
import com.signalcollect.triplerush.Expression.{ * => * }
import scala.collection.mutable.TreeSet
import com.signalcollect.interfaces.Inspectable
import com.signalcollect.triplerush.QueryParticle._

object SignalSet extends Enumeration with Serializable {
  val BoundSubject = Value
  val BoundPredicate = Value
  val BoundObject = Value
}

/**
 * This vertex represents part of the TripleRush index.
 * The edge representation can currently only be modified during graph loading.
 * After graph loading, the `optimizeEdgeRepresentation` has to be called.
 * Query processing can only start once the edge representation has been optimized.
 */
class IndexVertex(id: TriplePattern) extends PatternVertex[Any, Any](id) with Inspectable[TriplePattern, Any] {

  override def targetIds: Traversable[TriplePattern] = {
    if (childDeltas != null) {
      childDeltas map (childPatternCreator)
    } else {
      // TODO: Give CompactIntSet a nicer API
      var childDeltasTemp = List[Int]()
      CompactIntSet.foreach(childDeltasOptimized, childDelta => childDeltasTemp = childDelta :: childDeltasTemp)
      childDeltasTemp map (childPatternCreator)
    }
  }

  override def expose: Map[String, Any] = {
    var extraInfo: Map[String, Any] = Map("outDegree" -> edgeCount)
    extraInfo ++= targetIds take 10 map { targetId => "outEdge sample" -> targetId }
    extraInfo
  }

  /**
   * Can be called anytime to compute the cardinalities.
   */
  def computeCardinality(graphEditor: GraphEditor[Any, Any]) {
    assert(childDeltasOptimized != null)
    cardinality = 0
    if (id == TriplePattern(*, *, *)) {
      cardinality = Int.MaxValue //TODO: Add comment about why MaxValue
    } else {
      CompactIntSet.foreach(childDeltasOptimized, childDelta => {
        val childPattern = childPatternCreator(childDelta)
        graphEditor.sendSignal(CardinalityRequest(null, id), childPattern, None)
      })
    }
  }

  @transient var cardinality: Int = _

  @transient var edgeCounter: Int = _

  @transient var childDeltas: TreeSet[Int] = _

  @transient var childDeltasOptimized: Array[Byte] = _

  @transient var childPatternCreator: Int => TriplePattern = _

  def optimizeEdgeRepresentation {
    childDeltasOptimized = CompactIntSet.create(childDeltas.toArray)
    childDeltas = null
  }

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    super.afterInitialization(graphEditor)
    childDeltas = TreeSet[Int]()
    childPatternCreator = id.childPatternRecipe
  }

  override def edgeCount = edgeCounter

  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = {
    childDeltas = TreeSet[Int]() // TODO: Make sure this still works as intended once we add index optimizations.
    val removed = edgeCount
    edgeCounter = 0
    removed
  }

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    assert(childDeltas != null)
    val placeholderEdge = e.asInstanceOf[PlaceholderEdge]
    val wasAdded = childDeltas.add(placeholderEdge.childDelta)
    if (wasAdded) {
      edgeCounter += 1
    }
    wasAdded
  }

  def processFullQuery(queryParticle: Array[Int], graphEditor: GraphEditor[Any, Any]) {
    val targetIdCount = edgeCount
    val totalTickets = tickets(queryParticle)
    val avg = math.abs(totalTickets) / targetIdCount
    val complete = avg > 0 && totalTickets > 0
    var extras = math.abs(totalTickets) % targetIdCount
    val averageTicketQuery = copyWithTickets(queryParticle, avg, complete)
    val aboveAverageTicketQuery = copyWithTickets(queryParticle, avg + 1, complete)
    CompactIntSet.foreach(childDeltasOptimized, childDelta => {
      val targetId = childPatternCreator(childDelta)
      if (extras > 0) {
        graphEditor.sendSignal(aboveAverageTicketQuery, targetId, None)
        extras -= 1
      } else if (complete) {
        graphEditor.sendSignal(averageTicketQuery, targetId, None)
      }
    })
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) = {
    signal match {
      case queryParticle: Array[Int] =>
        processFullQuery(queryParticle, graphEditor)
      case CardinalityRequest(forPattern: TriplePattern, requestor: AnyRef) =>
        graphEditor.sendSignal(CardinalityReply(forPattern, cardinality), requestor, None)
      case CardinalityReply(forPattern, patternCardinality) =>
        cardinality += patternCardinality
    }
    true
  }
}