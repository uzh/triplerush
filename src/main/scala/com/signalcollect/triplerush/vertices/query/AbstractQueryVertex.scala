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

package com.signalcollect.triplerush.vertices.query

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.PredicateStatsReply
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.vertices.BaseVertex
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.QueryIds

abstract class AbstractQueryVertex[StateType](
    val query: Seq[TriplePattern],
    val tickets: Long,
    val numberOfSelectVariables: Int) extends BaseVertex[StateType] {

  val numberOfPatternsInOriginalQuery: Int = query.length

  val expectedCardinalityReplies: Int = numberOfPatternsInOriginalQuery
  var receivedCardinalityReplies: Int = 0

  var cardinalities = Map.empty[TriplePattern, Long]

  var receivedTickets = 0l
  var complete = true

  var dispatchedQuery: Option[Array[Int]] = None

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    if (numberOfPatternsInOriginalQuery > 0) {
      val particle = QueryParticle(
        patterns = query,
        queryId = QueryIds.extractQueryIdFromLong(id),
        numberOfSelectVariables = numberOfSelectVariables,
        tickets = tickets)
      dispatchedQuery = Some(particle)
      graphEditor.sendSignal(particle, particle.routingAddress)
    } else {
      dispatchedQuery = None
      // All stats processed, but no results, we can safely remove the query vertex now.
      reportResultsAndRequestQueryVertexRemoval(graphEditor)
    }
  }

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]): Boolean = {
    signal match {
      case deliveredTickets: Long =>
        processTickets(deliveredTickets)
        if (receivedTickets == tickets) {
          reportResultsAndRequestQueryVertexRemoval(graphEditor)
        }
      case bindings: Array[_] =>
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        // TODO, Wrap the msg and also make this a long.
        handleResultCount(resultCount)
    }
    true
  }

  def reportResultsAndRequestQueryVertexRemoval(graphEditor: GraphEditor[Long, Any]) {
    reportResults
    requestQueryVertexRemoval(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)

  def processTickets(t: Long) {
    receivedTickets += { if (t < 0) -t else t } // inlined math.abs
    if (t < 0) {
      complete = false
    }
  }

  var queryVertexRemovalRequested = false

  def requestQueryVertexRemoval(graphEditor: GraphEditor[Long, Any]) {
    if (!queryVertexRemovalRequested) {
      graphEditor.removeVertex(id)
    }
    queryVertexRemovalRequested = true
  }

  var resultsReported = false

  def reportResults {
    resultsReported = true
  }

}
