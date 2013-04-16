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

import com.signalcollect._
import com.signalcollect.triplerush.Expression._
import java.util.Arrays
import scala.util.Sorting

class BindingIndexVertex(id: TriplePattern) extends PatternVertex(id) {

  def optimizeEdgeRepresentation {
    childDeltasOptimized = childDeltas.toArray
    Arrays.sort(childDeltasOptimized)
    childDeltas = null
  }

  override def edgeCount = edgeCounter

  var edgeCounter = 0

  var childDeltas = List[Int]()

  var childDeltasOptimized: Array[Int] = null //TODO: Figure out if this is more elegant using ArrayBuffer

  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = {
    childDeltas = List[Int]() // TODO: Make sure this still works as intended once we add index optimizations.
    edgeCount
  }

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    require(childDeltas != null)
    edgeCounter += 1
    val placeholderEdge = e.asInstanceOf[PlaceholderEdge]
    childDeltas = placeholderEdge.childDelta :: childDeltas
    true
  }

  protected val childPatternCreator = id.childPatternRecipe

  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  override def process(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    //TODO: Evaluate running the process function in parallel on all the queries.
    require(childDeltasOptimized != null)
    val nextPatternToMatch = query.unmatched.head
    if (nextPatternToMatch.isFullyBound) {
      // We are looking for a specific, fully bound triple pattern. This means that we have to do a binary search on the targetIds.
      if (patternExists(nextPatternToMatch)) {
        bindToTriplePattern(nextPatternToMatch, query, graphEditor)
      } else {
        graphEditor.sendSignal(query, query.queryId, None)
      }
    } else {
      // We need to bind the next pattern to all targetIds
      val targetIdCount = edgeCount
      val avg = query.tickets / targetIdCount
      val complete = avg > 0
      var extras = query.tickets % targetIdCount
      val averageTicketQuery = query.withTickets(avg, complete)
      val aboveAverageTicketQuery = query.withTickets(avg + 1, complete)
      for (childDelta <- childDeltasOptimized) {
        if (extras > 0) {
          bindToTriplePattern(childPatternCreator(childDelta), aboveAverageTicketQuery, graphEditor)
          extras -= 1
        } else if (complete) {
          bindToTriplePattern(childPatternCreator(childDelta), averageTicketQuery, graphEditor)
        }
      }
    }
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
    Arrays.binarySearch(childDeltasOptimized, toFind) >= 0
  }

  def bindToTriplePattern(triplePattern: TriplePattern, query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val boundQueryOption = query.bind(triplePattern)
    if (boundQueryOption.isDefined) {
      val boundQuery = boundQueryOption.get
      if (boundQuery.unmatched.isEmpty) {
        // Query successful, send to query vertex.
        graphEditor.sendSignal(boundQuery, boundQuery.queryId, None)
      } else {
        // Query not complete yet, route onwards.
        graphEditor.sendSignal(boundQuery, boundQuery.unmatched.head.routingAddress, None)
      }
    } else {
      // Failed to bind, send to query vertex.
      graphEditor.sendSignal(query, query.queryId, None)
    }
  }

}