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
import com.signalcollect.triplerush.CardinalityCache
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.PredicateStatsCache
import com.signalcollect.triplerush.PredicateStatsReply
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.vertices.BaseVertex
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.QueryIds

// TODO: Short-circuit query for 0 cardinality patterns in optimizer.
abstract class AbstractQueryVertex[StateType](
    val query: Seq[TriplePattern],
    val tickets: Long,
    val numberOfSelectVariables: Int,
    val optimizer: Option[Optimizer]) extends BaseVertex[StateType] {

  val numberOfPatternsInOriginalQuery: Int = query.length

  val queryTickets = Long.MaxValue

  val queryTicketsReceived = new TicketSynchronization(s"queryTicketsReceived[${query.mkString}]", queryTickets)

  // Both predicate and cardinality stats.
  val statsReceived = new TicketSynchronization(s"statsReceived[${query.mkString}]", 2 * numberOfPatternsInOriginalQuery)

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    optimizingStartTime = System.nanoTime
    //TODO: Should we run an optimizer even for one-pattern queries?
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
      query.foreach(gatherStatsForPattern(_, graphEditor))
      statsReceived.onSuccess {
        case _ => handleQueryDispatch(graphEditor)
      }
    } else {
      // Dispatch the query directly.
      optimizingDuration = System.nanoTime - optimizingStartTime
      if (numberOfPatternsInOriginalQuery > 0) {
        val particle = QueryParticle(
          patterns = query,
          queryId = QueryIds.extractQueryIdFromLong(id),
          numberOfSelectVariables = numberOfSelectVariables,
          tickets = tickets)
        graphEditor.sendSignal(particle, particle.routingAddress)
      } else {
        // All stats processed, but no results, we can safely remove the query vertex now.
        reportResultsAndRequestQueryVertexRemoval(true, graphEditor)
      }
    }
  }

  var requestedIndexCardinalities = Set.empty[Long]
  def gatherStatsForPattern(triplePattern: TriplePattern, graphEditor: GraphEditor[Long, Any]) {
    val patternWithWildcards = triplePattern.withVariablesAsWildcards
    val cardinalityIndexId = patternWithWildcards.routingAddress
    val p = patternWithWildcards.p

    if (p == 0) {
      statsReceived.receivedTickets(1)
    } else {
      PredicateStatsCache(p) match {
        case Some(c) =>
          statsReceived.receivedTickets(1)
        case None =>
          val pIndexAddress = EfficientIndexPattern(0, p, 0)
          if (requestedIndexCardinalities.contains(pIndexAddress)) {
            statsReceived.receivedTickets(1)
          } else {
            requestedIndexCardinalities += pIndexAddress
            graphEditor.sendSignal(CardinalityRequest(triplePattern, id), pIndexAddress)
          }
      }
    }

    CardinalityCache(patternWithWildcards) match {
      case Some(c) =>
        statsReceived.receivedTickets(1)
      case None =>
        if (requestedIndexCardinalities.contains(cardinalityIndexId)) {
          statsReceived.receivedTickets(1)
        } else {
          requestedIndexCardinalities += cardinalityIndexId
          graphEditor.sendSignal(CardinalityRequest(triplePattern, id), cardinalityIndexId)
        }
    }

  }

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]): Boolean = {
    println(s"got signal $signal")
    signal match {
      case deliveredTickets: Long =>
        println(s"GOT QUERY TICKETS! $deliveredTickets")
        queryTicketsReceived.receivedTickets(deliveredTickets)
      case bindings: Array[_] =>
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        // TODO, Wrap the msg and also make this a long.
        handleResultCount(resultCount)
      case CardinalityReply(forPattern, cardinality) =>
        //received cardinality reply from: forPattern
        CardinalityCache.add(forPattern.withVariablesAsWildcards, cardinality)
        statsReceived.receivedTickets(1)
      case PredicateStatsReply(forPattern, cardinality, predicateStats) =>
        //received cardinalityandedgecount reply from: forPattern
        CardinalityCache.add(forPattern.withVariablesAsWildcards, cardinality)
        val pIndexForPattern = forPattern.p
        PredicateStatsCache.add(pIndexForPattern, predicateStats)
        statsReceived.receivedTickets(1)
    }
    true
  }

  def handleQueryDispatch(graphEditor: GraphEditor[Long, Any]) {
    val queryOption = optimizeQuery
    queryOption match {
      case Some(q) =>
        queryTicketsReceived.onSuccess {
          case _ => reportResultsAndRequestQueryVertexRemoval(true, graphEditor)
        }
        queryTicketsReceived.onFailure {
          case _ => reportResultsAndRequestQueryVertexRemoval(false, graphEditor)
        }
        graphEditor.sendSignal(q, q.routingAddress)
      case None =>
        reportResultsAndRequestQueryVertexRemoval(true, graphEditor)
    }
  }

  def reportResultsAndRequestQueryVertexRemoval(completeExecution: Boolean, graphEditor: GraphEditor[Long, Any]) {
    reportResults(completeExecution)
    requestQueryVertexRemoval(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)

  def optimizeQuery: Option[Array[Int]] = {
    val cardinalities = query.zip(query.map { pattern =>
      CardinalityCache.implementation(pattern.withVariablesAsWildcards)
    }).toMap
    val optimizedPatterns = optimizer.get.optimize(cardinalities, PredicateStatsCache.implementation)
    optimizingDuration = System.nanoTime - optimizingStartTime
    if (optimizedPatterns.length > 0) {
      val optimizedQuery = QueryParticle(
        patterns = optimizedPatterns,
        queryId = QueryIds.extractQueryIdFromLong(id),
        numberOfSelectVariables = numberOfSelectVariables,
        tickets = tickets)
      Some(optimizedQuery)
    } else {
      None
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

  def reportResults(completeExecution: Boolean): Unit = {
    resultsReported = true
  }

}
