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

abstract class AbstractQueryVertex[StateType](
  val query: Seq[TriplePattern],
  val tickets: Long,
  val numberOfSelectVariables: Int,
  val optimizer: Option[Optimizer]) extends BaseVertex[StateType] {

  val numberOfPatternsInOriginalQuery: Int = query.length

  val expectedCardinalityReplies: Int = numberOfPatternsInOriginalQuery
  var receivedCardinalityReplies: Int = 0

  var cardinalities = Map.empty[TriplePattern, Long]

  var receivedTickets = 0l
  var complete = true

  var dispatchedQuery: Option[Array[Int]] = None

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {

    optimizingStartTime = System.nanoTime
    //TODO: Should we run an optimizer even for one-pattern queries?
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
      gatherStatistics(graphEditor)
    } else {
      // Dispatch the query directly.
      optimizingDuration = System.nanoTime - optimizingStartTime
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
  }

  def gatherZeroPredicateStatsForPattern(triplePattern: TriplePattern, graphEditor: GraphEditor[Long, Any]) {
    val patternWithWildcards = triplePattern.withVariablesAsWildcards
    val fromCache = CardinalityCache(patternWithWildcards)
    val cardinalityInCache = fromCache.isDefined
    if (cardinalityInCache) {
      handleCardinalityReply(triplePattern, fromCache.get)
    } else {
      val responsibleIndexId = patternWithWildcards.routingAddress
      // Sending cardinality request to responsible Index.
      graphEditor.sendSignal(CardinalityRequest(triplePattern, id), responsibleIndexId)
    }
  }

  var requestedPredicateStats = Set.empty[Int]

  def gatherPredicateStatsForPattern(triplePattern: TriplePattern, graphEditor: GraphEditor[Long, Any]) {
    val patternWithWildcards = triplePattern.withVariablesAsWildcards
    val pIndexForPattern = patternWithWildcards.p
    val fromCache = CardinalityCache(patternWithWildcards)
    val predicateStats = PredicateStatsCache(pIndexForPattern)
    val cardinalityInCache = fromCache.isDefined
    val predicateStatsInCache = predicateStats.isDefined

    if (cardinalityInCache && predicateStatsInCache) {
      // Answer with stats from cache.
      handleCardinalityReply(triplePattern, fromCache.get)
    } else if (cardinalityInCache && !requestedPredicateStats.contains(pIndexForPattern)) {
      // Need to gather predicate stats.
      requestedPredicateStats += pIndexForPattern
      handleCardinalityReply(triplePattern, fromCache.get)
      graphEditor.sendSignal(CardinalityRequest(triplePattern, id), EfficientIndexPattern(0, pIndexForPattern, 0))
    } else if (predicateStatsInCache) {
      // Need to gather cardinality stats.
      val responsibleIndexId = patternWithWildcards.routingAddress
      graphEditor.sendSignal(CardinalityRequest(triplePattern, id), responsibleIndexId)
    } else {
      // Need to gather all stats.
      val responsibleIndexId = patternWithWildcards.routingAddress
      val pIndex = TriplePattern(0, pIndexForPattern, 0)
      graphEditor.sendSignal(CardinalityRequest(triplePattern, id), responsibleIndexId)
      if (!requestedPredicateStats.contains(pIndexForPattern) && pIndex != responsibleIndexId) {
        requestedPredicateStats += pIndexForPattern
        graphEditor.sendSignal(CardinalityRequest(triplePattern, id), pIndex.routingAddress)
      }
    }
  }

  def gatherStatistics(graphEditor: GraphEditor[Long, Any]) {
    // Gather pattern cardinalities.
    query foreach { triplePattern =>
      val pIndexForPattern = triplePattern.p
      if (pIndexForPattern > 0) {
        gatherPredicateStatsForPattern(triplePattern, graphEditor)
      } else {
        gatherZeroPredicateStatsForPattern(triplePattern, graphEditor)
      }
    }
    if (areStatsGathered) {
      handleQueryDispatch(graphEditor)
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
      case CardinalityReply(forPattern, cardinality) =>
        //received cardinality reply from: forPattern
        CardinalityCache.add(forPattern.withVariablesAsWildcards, cardinality)
        handleCardinalityReply(forPattern, cardinality)
        if (areStatsGathered) {
          handleQueryDispatch(graphEditor)
        }
      case PredicateStatsReply(forPattern, cardinality, predicateStats) =>
        //received cardinalityandedgecount reply from: forPattern
        CardinalityCache.add(forPattern.withVariablesAsWildcards, cardinality)
        val pIndexForPattern = forPattern.p
        PredicateStatsCache.add(pIndexForPattern, predicateStats)
        handleCardinalityReply(forPattern, cardinality)
        if (areStatsGathered) {
          handleQueryDispatch(graphEditor)
        }
    }
    true
  }

  def areStatsGathered: Boolean = {
    val expectedReplies = expectedCardinalityReplies + requestedPredicateStats.size
    //println(s"$id: q: ${querySpecification.unmatched}	$receivedCardinalityReplies/$expectedReplies")
    expectedReplies == receivedCardinalityReplies
  }

  def handleQueryDispatch(graphEditor: GraphEditor[Long, Any]) {
    if (queryMightHaveResults) {
      dispatchedQuery = optimizeQuery
      if (dispatchedQuery.isDefined) {
        graphEditor.sendSignal(
          dispatchedQuery.get,
          dispatchedQuery.get.routingAddress)
      } else {
        reportResultsAndRequestQueryVertexRemoval(graphEditor)
      }
    } else {
      reportResultsAndRequestQueryVertexRemoval(graphEditor)
    }
  }

  def reportResultsAndRequestQueryVertexRemoval(graphEditor: GraphEditor[Long, Any]) {
    reportResults
    requestQueryVertexRemoval(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)

  var queryMightHaveResults = true

  def handleCardinalityReply(
    forPattern: TriplePattern,
    cardinality: Long) {
    cardinalities += forPattern -> cardinality
    receivedCardinalityReplies += 1
    if (cardinality == 0) {
      // 0 cardinality => no results => we're done.
      queryMightHaveResults = false
      reportResults
    }
  }

  def optimizeQuery: Option[Array[Int]] = {
    val optimizedPatterns = optimizer.get.optimize(
      cardinalities, PredicateStatsCache.implementation)
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
