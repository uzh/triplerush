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

import scala.concurrent.Promise
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.vertices.BaseVertex
import com.signalcollect.triplerush.Cardinalities
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.CardinalityAndEdgeCountReply
import com.signalcollect.triplerush.EdgeCounts
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.ObjectCounts
import com.signalcollect.triplerush.SubjectCounts

abstract class AbstractQueryVertex[StateType](
  val querySpecification: QuerySpecification,
  val optimizer: Option[Optimizer]) extends BaseVertex[Int, Any, StateType] {

  val expectedTickets = querySpecification.tickets
  val numberOfPatternsInOriginalQuery = querySpecification.unmatched.length

  val expectedCardinalityReplies = numberOfPatternsInOriginalQuery
  var receivedCardinalityReplies = 0

  var receivedTickets = 0l
  var complete = true

  var cardinalities = Map.empty[TriplePattern, Long]
  var subjectCounts = Map.empty[Int, Long]
  var maxObjectCounts = Map.empty[Int, Long]
  var maxSubjectCounts = Map.empty[Int, Long]

  var dispatchedQuery: Option[Array[Int]] = None

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    optimizingStartTime = System.nanoTime
    //TODO: Should we run an optimizer even for one-pattern queries?
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
      gatherStatistics(graphEditor)
    } else {
      // Dispatch the query directly.
      optimizingDuration = System.nanoTime - optimizingStartTime
      if (numberOfPatternsInOriginalQuery > 0) {
        dispatchedQuery = Some(QueryParticle.fromSpecification(id, querySpecification))
        graphEditor.sendSignal(dispatchedQuery.get, dispatchedQuery.get.routingAddress, None)
      } else {
        dispatchedQuery = None
        // All stats processed, but no results, we can safely remove the query vertex now.
        reportResults
        requestQueryVertexRemoval(graphEditor)
      }
    }
  }

  def gatherZeroPredicateStatsForPattern(triplePattern: TriplePattern, graphEditor: GraphEditor[Any, Any]) {
    val patternWithWildcards = triplePattern.withVariablesAsWildcards
    val fromCache = Cardinalities(patternWithWildcards)
    val cardinalityInCache = fromCache.isDefined
    if (cardinalityInCache) {
      handleCardinalityReply(triplePattern, fromCache.get, graphEditor)
    } else {
      val responsibleIndexId = patternWithWildcards.routingAddress
      responsibleIndexId match {
        case TriplePattern(0, 0, 0) =>
          // Root vertex.
          handleCardinalityReply(triplePattern, Int.MaxValue, graphEditor)
        case other =>
          // Sending cardinalityrequest to responsible Index.
          graphEditor.sendSignal(CardinalityRequest(triplePattern, id), responsibleIndexId, None)
      }
    }
  }

  var requestedPredicateStats = Set.empty[Int]

  def gatherPredicateStatsForPattern(triplePattern: TriplePattern, graphEditor: GraphEditor[Any, Any]) {
    val patternWithWildcards = triplePattern.withVariablesAsWildcards
    val pIndexForPattern = patternWithWildcards.p
    val fromCache = Cardinalities(patternWithWildcards)
    val edgeCountsCache = EdgeCounts(pIndexForPattern)
    val objectCountsCache = ObjectCounts(pIndexForPattern)
    val subjectCountsCache = SubjectCounts(pIndexForPattern)
    val cardinalityInCache = fromCache.isDefined
    val predicateStatsInCache = edgeCountsCache.isDefined && subjectCountsCache.isDefined && objectCountsCache.isDefined
    if (cardinalityInCache && predicateStatsInCache) {
      // Answer with stats from cache.
      handlePredicateStatsReply(triplePattern.p, edgeCountsCache.get, objectCountsCache.get, subjectCountsCache.get, graphEditor)
      handleCardinalityReply(triplePattern, fromCache.get, graphEditor)
    } else {
      // At least one of the stats not in cache, need to gather the missing ones.
      val responsibleIndexId = patternWithWildcards.routingAddress
      responsibleIndexId match {
        case TriplePattern(0, 0, 0) =>
          // Root vertex.
          handleCardinalityReply(triplePattern, Int.MaxValue, graphEditor)
        case other =>
          // Sending cardinalityrequest to responsible Index.
          graphEditor.sendSignal(CardinalityRequest(triplePattern, id), responsibleIndexId, None)
          if (!predicateStatsInCache && !requestedPredicateStats.contains(pIndexForPattern)) {
            // Also sending cardinality request for pIndex.
            requestedPredicateStats += pIndexForPattern
            graphEditor.sendSignal(CardinalityRequest(triplePattern, id), TriplePattern(0, pIndexForPattern, 0), None)
          } else if (edgeCountsCache.isDefined && objectCountsCache.isDefined && subjectCountsCache.isDefined) {
            // Edge count cache is populated.
            subjectCounts += pIndexForPattern -> edgeCountsCache.get
            maxObjectCounts += pIndexForPattern -> objectCountsCache.get
            maxSubjectCounts += pIndexForPattern -> subjectCountsCache.get
          }
      }
    }
  }

  def gatherStatistics(graphEditor: GraphEditor[Any, Any]) {
    // Gather pattern cardinalities.
    querySpecification.unmatched foreach { triplePattern =>
      val pIndexForPattern = triplePattern.p
      if (pIndexForPattern > 0) {
        gatherPredicateStatsForPattern(triplePattern, graphEditor)
      } else {
        gatherZeroPredicateStatsForPattern(triplePattern, graphEditor)
      }
    }
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case tickets: Long =>
        processTickets(tickets)
        if (receivedTickets == expectedTickets) {
          reportResults
          requestQueryVertexRemoval(graphEditor)
        }
      case bindings: Array[_] =>
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        // TODO, Wrap the msg and also make this a long.
        handleResultCount(resultCount)
      case CardinalityReply(forPattern, cardinality) =>
        //received cardinality reply from: forPattern
        handleCardinalityReply(forPattern, cardinality, graphEditor)
      case CardinalityAndEdgeCountReply(forPattern, cardinality, edgeCount, maxObjectCount, maxSubjectCount) =>
        //received cardinalityandedgecount reply from: forPattern
        handlePredicateStatsReply(forPattern.p, edgeCount, maxObjectCount, maxSubjectCount, graphEditor)
        handleCardinalityReply(forPattern, cardinality, graphEditor)
    }
    true
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)

  def handlePredicateStatsReply(
    pIndexForPattern: Int,
    edgeCount: Long,
    maxObjectCount: Long,
    maxSubjectCount: Long,
    graphEditor: GraphEditor[Any, Any]) {
    subjectCounts += pIndexForPattern -> edgeCount
    maxObjectCounts += pIndexForPattern -> maxObjectCount
    maxSubjectCounts += pIndexForPattern -> maxSubjectCount
    EdgeCounts.add(pIndexForPattern, edgeCount)
    ObjectCounts.add(pIndexForPattern, maxObjectCount)
    SubjectCounts.add(pIndexForPattern, maxSubjectCount)
  }

  var queryMightHaveResults = true

  def handleCardinalityReply(
    forPattern: TriplePattern,
    cardinality: Long,
    graphEditor: GraphEditor[Any, Any]) = {
    cardinalities += forPattern -> cardinality
    Cardinalities.add(forPattern.withVariablesAsWildcards, cardinality)
    receivedCardinalityReplies += 1
    if (cardinality == 0) {
      // 0 cardinality => no results => we're done.
      queryMightHaveResults = false
      reportResults
    } else if (receivedCardinalityReplies == expectedCardinalityReplies) {
      if (queryMightHaveResults) {
        dispatchedQuery = optimizeQuery
        if (dispatchedQuery.isDefined) {
          graphEditor.sendSignal(dispatchedQuery.get, dispatchedQuery.get.routingAddress, None)
        }
      } else {
        reportResults
        requestQueryVertexRemoval(graphEditor)
      }
    }
  }

  def optimizeQuery: Option[Array[Int]] = {
    val optimizedPatterns = optimizer.get.optimize(cardinalities, subjectCounts, maxObjectCounts, maxSubjectCounts)
    optimizingDuration = System.nanoTime - optimizingStartTime
    if (optimizedPatterns.length > 0) {
      val optimizedQuery = QueryParticle.fromSpecification(id, querySpecification.withUnmatchedPatterns(optimizedPatterns))
      Some(optimizedQuery)
    } else {
      None
    }
  }

  def processTickets(t: Long) {
    receivedTickets += math.abs(t)
    if (t < 0) {
      complete = false
    }
  }

  var queryVertexRemovalRequested = false

  def requestQueryVertexRemoval(graphEditor: GraphEditor[Any, Any]) {
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
