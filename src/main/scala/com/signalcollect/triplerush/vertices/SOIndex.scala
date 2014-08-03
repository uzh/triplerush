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

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.util.SearchableIntSet
import com.signalcollect.triplerush.QueryIds

final class SOIndex(id: Long) extends SearchableIndexVertex(id)
  with Binding {

  override def onEdgeAdded(ge: GraphEditor[Long, Any]) {}

  /**
   * Need to check if the pattern is fully bound, then answer with appropriate cardinality.
   */
  override def handleCardinalityRequest(c: CardinalityRequest, graphEditor: GraphEditor[Long, Any]) {
    val pattern = c.forPattern
    if (pattern.isFullyBound) {
      val exists = childIdsContain(pattern.p)
      if (exists) {
        graphEditor.sendSignal(
          CardinalityReply(pattern, 1), c.requestor)
      } else {
        graphEditor.sendSignal(
          CardinalityReply(pattern, 0), c.requestor)
      }
    } else {
      super.handleCardinalityRequest(c, graphEditor)
    }
  }

  @inline def bindIndividualQuery(childDelta: Int, query: Array[Int]): Array[Int] = {
    query.bind(id.s, childDelta, id.o)
  }

  /**
   * Binds the queries to the pattern of this vertex and routes them to their
   * next destinations.
   */
  override def processQuery(query: Array[Int], graphEditor: GraphEditor[Long, Any]) {
    val patternP = query.lastPatternP
    if (patternP > 0 && query.lastPatternS > 0 && query.lastPatternO > 0) {
      // We are looking for a specific, fully bound triple pattern. This means that we have to do a binary search on the targetIds.
      if (childIdsContain(patternP)) {
        routeSuccessfullyBound(query.copyWithoutLastPattern, graphEditor)
      } else {
        // Failed query
        val queryVertexId = QueryIds.embedQueryIdInLong(query.queryId)
        graphEditor.sendSignal(query.tickets, queryVertexId)
      }
    } else {
      // We need to bind the next pattern to all targetIds.
      bindQueryToAllTriples(query, graphEditor)
    }
  }

}
