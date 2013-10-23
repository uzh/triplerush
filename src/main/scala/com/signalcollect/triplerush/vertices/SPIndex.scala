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

package com.signalcollect.triplerush.vertices

import com.signalcollect.triplerush.QueryParticle._
import com.signalcollect.triplerush.TriplePattern

class SPIndex(id: TriplePattern) extends OptimizedIndexVertex(id)
  with Binding {

  assert(id.s != 0 && id.p != 0 && id.o == 0)
  
  @inline def bindIndividualQuery(childDelta: Int, query: Array[Int]): Array[Int] = {
    query.bind(id.s, id.p, childDelta)
  }
}

//
//  def bindQueryParticle(queryParticle: Array[Int], edges: Int, graphEditor: GraphEditor[Any, Any]) {
//    val totalTickets = queryParticle.tickets
//    val avg = math.abs(totalTickets) / edges
//    val complete = avg > 0 && totalTickets > 0
//    var extras = math.abs(totalTickets) % edges
//    val averageTicketQuery = queryParticle.copyWithTickets(avg, complete)
//    val aboveAverageTicketQuery = queryParticle.copyWithTickets(avg + 1, complete)
//    IntSet(childDeltasOptimized).foreach(childDelta =>
//      if (extras > 0) {
//        extras -= 1
//        bindToTriplePattern(childDelta, aboveAverageTicketQuery, graphEditor)
//      } else if (complete) {
//        bindToTriplePattern(childDelta, averageTicketQuery, graphEditor)
//      })
//  }
//
//  @inline def bindParticle(childDelta: Int, queryParticle: Array[Int]): Array[Int] = {
//    queryParticle.bind(childDelta, id.p, id.o)
//  }
//}
//
//class SOBindingIndex(id: TriplePattern) extends BindingIndexVertex(id) {
//
//  @transient var childDeltasOptimized: Array[Int] = _
//
//  /**
//   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
//   */
//  def bindQuery(queryParticle: Array[Int], graphEditor: GraphEditor[Any, Any]) {
//    val patternS = queryParticle.lastPatternS
//    val patternP = queryParticle.lastPatternP
//    val patternO = queryParticle.lastPatternO
//    if (patternS > 0 && patternP > 0 && patternO > 0) {
//      // We are looking for a specific, fully bound triple pattern. This means that we have to do a binary search on the targetIds.
//      if (patternExists(patternS, patternP, patternO)) {
//        routeSuccessfullyBound(queryParticle.copyWithoutLastPattern, graphEditor)
//      } else {
//        // Failed query
//        graphEditor.sendSignal(queryParticle.tickets, queryParticle.queryId, None)
//      }
//    } else {
//      // We need to bind the next pattern to all targetIds
//      bindQueryParticle(queryParticle, edgeCount, graphEditor)
//    }
//  }
//
//  /**
//   * Checks if `toFind` is in `values` using binary search.
//   */
//  @inline def find(toFind: Int): Boolean = {
//    //  Arrays.binarySearch(childDeltasOptimized, toFind) >= 0
//    var lower = 0
//    var upper = childDeltasOptimized.length - 1
//    while (lower <= upper) {
//      val mid = lower + (upper - lower) / 2
//      val midItem = childDeltasOptimized(mid)
//      if (midItem < toFind) {
//        lower = mid + 1
//      } else if (midItem > toFind) {
//        upper = mid - 1
//      } else {
//        return true
//      }
//    }
//    false
//  }
//
//  override def targetIds: Traversable[TriplePattern] = {
//    if (childDeltas != null) {
//      childDeltas map (id.childPatternRecipe)
//    } else {
//      childDeltasOptimized map (id.childPatternRecipe)
//    }
//  }
//
//  /**
//   * Changes the edge representation from a List to a sorted Array.
//   */
//  def optimizeEdgeRepresentation {
//    val childDeltasArray = childDeltas.toArray
//    Arrays.sort(childDeltasArray)
//    childDeltasOptimized = childDeltasArray
//    childDeltas = null
//  }
//
//  @inline def patternExists(s: Int, p: Int, o: Int): Boolean = {
//    find(p)
//  }
//
//  @inline def bindParticle(childDelta: Int, queryParticle: Array[Int]): Array[Int] = {
//    queryParticle.bind(id.s, childDelta, id.o)
//  }
//
//  def bindQueryParticle(queryParticle: Array[Int], edges: Int, graphEditor: GraphEditor[Any, Any]) {
//    val totalTickets = queryParticle.tickets
//    val avg = math.abs(totalTickets) / edges
//    val complete = avg > 0 && totalTickets > 0
//    var extras = math.abs(totalTickets) % edges
//    val averageTicketQuery = queryParticle.copyWithTickets(avg, complete)
//    val aboveAverageTicketQuery = queryParticle.copyWithTickets(avg + 1, complete)
//    var i = 0
//    val arrayLength = childDeltasOptimized.length
//    while (i < arrayLength) {
//      val childDelta = childDeltasOptimized(i)
//      if (extras > 0) {
//        extras -= 1
//        bindToTriplePattern(childDelta, aboveAverageTicketQuery, graphEditor)
//      } else if (complete) {
//        bindToTriplePattern(childDelta, averageTicketQuery, graphEditor)
//      }
//      i += 1
//    }
//  }
//
//}
