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

import java.util.Arrays
import scala.collection.mutable.TreeSet
import com.signalcollect.GraphEditor
import com.signalcollect.interfaces.Inspectable
import com.signalcollect.DefaultEdge
import com.signalcollect.Edge
import com.signalcollect.triplerush.QueryParticle._
import com.signalcollect.examples.CompactIntSet

class TripleIndexEdge(override val sourceId: TriplePattern, override val targetId: TriplePattern) extends DefaultEdge[TriplePattern](targetId) {
  def signal = Unit
}

class SPBindingIndex(id: TriplePattern) extends BindingIndexVertex(id) {

  @transient var childDeltasOptimized: Array[Byte] = _

  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  def bindQuery(queryParticle: Array[Int], graphEditor: GraphEditor[Any, Any]) {
    // We need to bind the next pattern to all targetIds
    bindQueryParticle(queryParticle, edgeCount, graphEditor)
  }

  def bindQueryParticle(queryParticle: Array[Int], edges: Int, graphEditor: GraphEditor[Any, Any]) {
    val totalTickets = queryParticle.tickets
    val avg = math.abs(totalTickets) / edges
    val complete = avg > 0 && totalTickets > 0
    var extras = math.abs(totalTickets) % edges
    val averageTicketQuery = queryParticle.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = queryParticle.copyWithTickets(avg + 1, complete)
    CompactIntSet.foreach(childDeltasOptimized, childDelta =>
      if (extras > 0) {
        extras -= 1
        bindToTriplePattern(childDelta, aboveAverageTicketQuery, graphEditor)
      } else if (complete) {
        bindToTriplePattern(childDelta, averageTicketQuery, graphEditor)
      })
  }

  override def targetIds: Traversable[TriplePattern] = {
    if (childDeltas != null) {
      childDeltas map (id.childPatternRecipe)
    } else {
      // TODO: Give CompactIntSet a nicer API
      var childDeltasTemp = List[Int]()
      CompactIntSet.foreach(childDeltasOptimized, childDelta => childDeltasTemp = childDelta :: childDeltasTemp)
      childDeltasTemp map (id.childPatternRecipe)
    }
  }

  def optimizeEdgeRepresentation {
    childDeltasOptimized = CompactIntSet.create(childDeltas.toArray)
    childDeltas = null
  }

  @inline def bindParticle(childDelta: Int, queryParticle: Array[Int]): Array[Int] = {
    queryParticle.bind(id.s, id.p, childDelta)
  }
}

class POBindingIndex(id: TriplePattern) extends BindingIndexVertex(id) {

  @transient var childDeltasOptimized: Array[Byte] = _

  override def targetIds: Traversable[TriplePattern] = {
    if (childDeltas != null) {
      childDeltas map (id.childPatternRecipe)
    } else {
      // TODO: Give CompactIntSet a nicer API
      var childDeltasTemp = List[Int]()
      CompactIntSet.foreach(childDeltasOptimized, childDelta => childDeltasTemp = childDelta :: childDeltasTemp)
      childDeltasTemp map (id.childPatternRecipe)
    }
  }

  def optimizeEdgeRepresentation {
    childDeltasOptimized = CompactIntSet.create(childDeltas.toArray)
    childDeltas = null
  }

  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  def bindQuery(queryParticle: Array[Int], graphEditor: GraphEditor[Any, Any]) {
    // We need to bind the next pattern to all targetIds
    bindQueryParticle(queryParticle, edgeCount, graphEditor)
  }

  def bindQueryParticle(queryParticle: Array[Int], edges: Int, graphEditor: GraphEditor[Any, Any]) {
    val totalTickets = queryParticle.tickets
    val avg = math.abs(totalTickets) / edges
    val complete = avg > 0 && totalTickets > 0
    var extras = math.abs(totalTickets) % edges
    val averageTicketQuery = queryParticle.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = queryParticle.copyWithTickets(avg + 1, complete)
    CompactIntSet.foreach(childDeltasOptimized, childDelta =>
      if (extras > 0) {
        extras -= 1
        bindToTriplePattern(childDelta, aboveAverageTicketQuery, graphEditor)
      } else if (complete) {
        bindToTriplePattern(childDelta, averageTicketQuery, graphEditor)
      })
  }

  @inline def bindParticle(childDelta: Int, queryParticle: Array[Int]): Array[Int] = {
    queryParticle.bind(childDelta, id.p, id.o)
  }
}

class SOBindingIndex(id: TriplePattern) extends BindingIndexVertex(id) {

  @transient var childDeltasOptimized: Array[Int] = _

  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  def bindQuery(queryParticle: Array[Int], graphEditor: GraphEditor[Any, Any]) {
    val patternS = queryParticle.lastPatternS
    val patternP = queryParticle.lastPatternP
    val patternO = queryParticle.lastPatternO
    if (patternS > 0 && patternP > 0 && patternO > 0) {
      // We are looking for a specific, fully bound triple pattern. This means that we have to do a binary search on the targetIds.
      if (patternExists(patternS, patternP, patternO)) {
        routeSuccessfullyBound(queryParticle.copyWithoutLastPattern, graphEditor)
      } else {
        // Failed query
        graphEditor.sendSignal(queryParticle.tickets, queryParticle.queryId, None)
      }
    } else {
      // We need to bind the next pattern to all targetIds
      val edges = edgeCount
      bindQueryParticle(queryParticle, edges, graphEditor)
    }
  }

  /**
   * Checks if `toFind` is in `values` using binary search.
   */
  @inline def find(toFind: Int): Boolean = {
    //  Arrays.binarySearch(childDeltasOptimized, toFind) >= 0
    var lower = 0
    var upper = childDeltasOptimized.length - 1
    while (lower <= upper) {
      val mid = lower + (upper - lower) / 2
      val midItem = childDeltasOptimized(mid)
      if (midItem < toFind) {
        lower = mid + 1
      } else if (midItem > toFind) {
        upper = mid - 1
      } else {
        return true
      }
    }
    false
  }

  override def targetIds: Traversable[TriplePattern] = {
    if (childDeltas != null) {
      childDeltas map (id.childPatternRecipe)
    } else {
      childDeltasOptimized map (id.childPatternRecipe)
    }
  }

  /**
   * Changes the edge representation from a List to a sorted Array.
   */
  def optimizeEdgeRepresentation {
    val childDeltasArray = childDeltas.toArray
    Arrays.sort(childDeltasArray)
    childDeltasOptimized = childDeltasArray
    childDeltas = null
  }

  @inline def patternExists(s: Int, p: Int, o: Int): Boolean = {
    find(p)
  }

  @inline def bindParticle(childDelta: Int, queryParticle: Array[Int]): Array[Int] = {
    queryParticle.bind(id.s, childDelta, id.o)
  }

  def bindQueryParticle(queryParticle: Array[Int], edges: Int, graphEditor: GraphEditor[Any, Any]) {
    val totalTickets = queryParticle.tickets
    val avg = math.abs(totalTickets) / edges
    val complete = avg > 0 && totalTickets > 0
    var extras = math.abs(totalTickets) % edges
    val averageTicketQuery = queryParticle.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = queryParticle.copyWithTickets(avg + 1, complete)
    var i = 0
    val arrayLength = childDeltasOptimized.length
    while (i < arrayLength) {
      val childDelta = childDeltasOptimized(i)
      if (extras > 0) {
        extras -= 1
        bindToTriplePattern(childDelta, aboveAverageTicketQuery, graphEditor)
      } else if (complete) {
        bindToTriplePattern(childDelta, averageTicketQuery, graphEditor)
      }
      i += 1
    }
  }

}

abstract class BindingIndexVertex(id: TriplePattern)
  extends PatternVertex[Any, Any](id)
  with Inspectable[TriplePattern, Any] {

  def bindQuery(queryParticle: Array[Int], graphEditor: GraphEditor[Any, Any])

  def bindQueryParticle(queryParticle: Array[Int], edges: Int, graphEditor: GraphEditor[Any, Any])

  def bindParticle(childDelta: Int, queryParticle: Array[Int]): Array[Int]

  override def targetIds: Traversable[TriplePattern]

  /**
   * Changes the edge representation from a List to a sorted Array.
   */
  def optimizeEdgeRepresentation

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    super.afterInitialization(graphEditor)
    childDeltas = TreeSet[Int]()
  }

  override def edgeCount = edgeCounter

  def cardinality = edgeCounter

  @transient var edgeCounter: Int = _

  @transient var childDeltas: TreeSet[Int] = _

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

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) = {
    signal match {
      case queryParticle: Array[Int] =>
        bindQuery(queryParticle, graphEditor)
      case CardinalityRequest(pattern, requestor) =>
        graphEditor.sendSignal(CardinalityReply(pattern, cardinality), requestor, None)
    }
    true
  }

  def bindToTriplePattern(childDelta: Int,
    queryParticle: Array[Int],
    graphEditor: GraphEditor[Any, Any]) {
    val boundParticle = bindParticle(childDelta, queryParticle)
    if (boundParticle != null) {
      routeSuccessfullyBound(boundParticle, graphEditor)
    } else {
      // Failed to bind, send to query vertex.
      graphEditor.sendSignal(queryParticle.tickets, queryParticle.queryId, None)
    }
  }

  def routeSuccessfullyBound(boundParticle: Array[Int],
    graphEditor: GraphEditor[Any, Any]) {
    if (boundParticle.isResult) {
      // Query successful, send to query vertex.
      graphEditor.sendSignal(boundParticle, boundParticle.queryId, None)
    } else {
      // Query not complete yet, route onwards.
      //TODO: Avoid creating the extra triple pattern before computing the routing address.
      val s = boundParticle.lastPatternS
      val p = boundParticle.lastPatternP
      val o = boundParticle.lastPatternO
      val nextRoutingAddress = if (s > 0 && p > 0 && o > 0) {
        // TODO: Determine best way to check triple containment.
        TriplePattern(s, 0, o)
      } else {
        TriplePattern(math.max(s, 0), math.max(p, 0), math.max(o, 0))
      }
      graphEditor.sendSignal(boundParticle, nextRoutingAddress, None)
    }
  }

}
