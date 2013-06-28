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
import com.signalcollect.triplerush.Expression.{ * => * }
import com.signalcollect.interfaces.Inspectable
import com.signalcollect.DefaultEdge
import com.signalcollect.Edge

class TripleIndexEdge(override val sourceId: TriplePattern, override val targetId: TriplePattern) extends DefaultEdge[TriplePattern](targetId) {
  def signal = Unit
}

class BindingIndexVertex(id: TriplePattern) extends PatternVertex[Any, Any](id) with Inspectable[TriplePattern, Any] {

  override def targetIds: Traversable[TriplePattern] = {
    if (childDeltas != null) {
      childDeltas map (childPatternCreator)
    } else {
      childDeltasOptimized map (childPatternCreator)
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

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    super.afterInitialization(graphEditor)
    childDeltas = TreeSet[Int]()
    childPatternCreator = id.childPatternRecipe
  }

  override def edgeCount = edgeCounter

  def cardinality = edgeCounter

  @transient var edgeCounter: Int = _

  @transient var childDeltas: TreeSet[Int] = _

  @transient var childDeltasOptimized: Array[Int] = _

  @transient var childPatternCreator: Int => TriplePattern = _

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

  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  def bindQuery(query: QueryParticle, graphEditor: GraphEditor[Any, Any]) {
    //TODO: Evaluate running the process function in parallel on all the queries.
    assert(childDeltasOptimized != null)
    val nextPatternToMatch = query.lastPattern
    if (nextPatternToMatch.isFullyBound) {
      // We are looking for a specific, fully bound triple pattern. This means that we have to do a binary search on the targetIds.
      if (patternExists(nextPatternToMatch)) {
        bindToTriplePattern(nextPatternToMatch, query, graphEditor)
      } else {
        // Failed query
        graphEditor.sendSignal(query.tickets, query.queryId, None)
      }
    } else {
      // We need to bind the next pattern to all targetIds
      val edges = edgeCount
      bind(query, edges, graphEditor)
    }
  }

  def bind(query: QueryParticle, edges: Int, graphEditor: GraphEditor[Any, Any]) {
    val avg = math.abs(query.tickets) / edges
    val complete = avg > 0 && query.tickets > 0
    var extras = math.abs(query.tickets) % edges
    val averageTicketQuery = query.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = query.copyWithTickets(avg + 1, complete)
    for (childDelta <- childDeltasOptimized) {
      if (extras > 0) {
        extras -= 1
        bindToTriplePattern(childPatternCreator(childDelta), aboveAverageTicketQuery, graphEditor)
      } else if (complete) {
        bindToTriplePattern(childPatternCreator(childDelta), averageTicketQuery, graphEditor)
      }
    }
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) = {
    signal match {
      case query: QueryParticle =>
        bindQuery(query, graphEditor)
      case CardinalityRequest(pattern, requestor) =>
        graphEditor.sendSignal(CardinalityReply(pattern, cardinality), requestor, None)
    }
    true
  }

  def patternExists(tp: TriplePattern): Boolean = {
    if (id.s == *) {
      find(tp.s)
    } else if (id.p == *) {
      find(tp.p)
    } else if (id.o == *) {
      find(tp.o)
    } else {
      throw new UnsupportedOperationException(s"The vertex with id $id should not be a BindingIndexVertex.")
    }
  }

  /**
   * Checks if `toFind` is in `values` using binary search.
   */
  def find(toFind: Int): Boolean = {
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

  def bindToTriplePattern(triplePattern: TriplePattern, query: QueryParticle, graphEditor: GraphEditor[Any, Any]) {
    val boundQuery = query.bind(triplePattern)
    if (boundQuery != null) {
      if (boundQuery.isResult) {
        // Query successful, send to query vertex.
        graphEditor.sendSignal(boundQuery, boundQuery.queryId, None)
      } else {
        // Query not complete yet, route onwards.
        graphEditor.sendSignal(boundQuery, boundQuery.lastPattern.routingAddress, None)
      }
    } else {
      // Failed to bind, send to query vertex.
      graphEditor.sendSignal(query.tickets, query.queryId, None)
    }
  }

}