/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

import scala.concurrent.Promise
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.QuerySpecification
import scala.concurrent.Future
import scala.collection.mutable.MapBuilder
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import com.signalcollect.triplerush.optimizers.AdvancedOptimizer
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.optimizers.GreedyCardinalityOptimizer

final class AdvancedPlanningQueryVertex(
  val querySpecification: QuerySpecification,
  val resultPromise: Promise[Traversable[Array[Int]]],
  val statsPromise: Promise[Map[Any, Any]]) extends BaseVertex[Int, Any, ArrayOfArraysTraversable] {

  val id = QueryIds.nextQueryId

  val expectedTickets = querySpecification.tickets
  val numberOfPatternsInOriginalQuery = querySpecification.unmatched.length
  var receivedCardinalityReplies = 0
  var queryDone = false
  var queryCopyCount: Long = 0
  var receivedTickets: Long = 0
  var complete = true

  var pairwisePatternsExpected: Int = _
  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  var pairwisePatternCardinalities: Map[Set[TriplePattern], Option[Long]] = _
  var cardinalities: Map[TriplePattern, Long] = _
  var dispatchedQuery: Option[Array[Int]] = None

  def uniquePairs[G](sequence: Seq[G]): Seq[Seq[G]] = {
    val length = sequence.length
    for {
      i <- 0 until length
      j <- i + 1 until length
    } yield Seq(sequence(i), sequence(j))
  }

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = new ArrayOfArraysTraversable
    optimizingStartTime = System.nanoTime
    if (numberOfPatternsInOriginalQuery > 1) {
      cardinalities = Map[TriplePattern, Long]()
      // Gather pattern cardinalities.
      querySpecification.unmatched foreach (triplePattern => {
        val responsibleIndexId = triplePattern.routingAddress
        responsibleIndexId match {
          case root @ TriplePattern(0, 0, 0) =>
            handleCardinalityReply(triplePattern, Int.MaxValue, graphEditor)
          case other =>
            graphEditor.sendSignal(
              CardinalityRequest(triplePattern, id),
              responsibleIndexId, None)
        }
      })
    } else {
      // Dispatch the query directly.
      optimizingDuration = System.nanoTime - optimizingStartTime
      if (numberOfPatternsInOriginalQuery > 0) {
        dispatchedQuery = Some(QueryParticle.fromSpecification(id, querySpecification))
        dispatchQuery(graphEditor)
      } else {
        dispatchedQuery = None
        queryDone(graphEditor)
      }
    }
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case tickets: Long =>
        queryCopyCount += 1
        processTickets(tickets)
        if (receivedTickets == expectedTickets) {
          queryDone(graphEditor)
        }
      case bindings: Array[_] =>
        queryCopyCount += 1
        state.add(bindings.asInstanceOf[Array[Array[Int]]])
      case ResultCount(patterns, count) =>
        handlePairwiseStatsReply(patterns, count, graphEditor)
        if (allPairwiseStatisticsReceived) {
          optimizingDuration = System.nanoTime - optimizingStartTime
          dispatchedQuery = optimizeQuery
          dispatchQuery(graphEditor)
        }
      case CardinalityReply(forPattern, cardinality) =>
        handleCardinalityReply(forPattern, cardinality, graphEditor)
        if (allCardinalitiesReceived) {
          if (numberOfPatternsInOriginalQuery > 2) {
            pairwisePatternCardinalities = Map[Set[TriplePattern], Option[Long]]()
            // Pattern with lower cardinality is always first.
            val pairwisePatterns = uniquePairs(cardinalities.toArray.sortBy(_._2).map(_._1))
            pairwisePatternsExpected = pairwisePatterns.length
            for (patterns <- pairwisePatterns) {
              val Seq(first, second) = patterns
              if (!first.variableSet.intersect(second.variableSet).isEmpty) {
                // Only compute join cardinality if the patterns share at least one variable.
                val resultCountPromise = Promise[Option[Long]]()
                graphEditor.addVertex(new SimpleResultCountingQueryVertex(id, QuerySpecification(patterns)))
              } else {
                // Assume the worst when patterns do not share a variable.
                pairwisePatternCardinalities += ((patterns.toSet, None))
              }
            }
          } else {
            optimizingDuration = System.nanoTime - optimizingStartTime
            dispatchedQuery = optimizeQuery
            dispatchQuery(graphEditor)
          }
        }
    }
    true
  }

  var allCardinalitiesReceived = false
  var allPairwiseStatisticsReceived = false

  def handlePairwiseStatsReply(patterns: Set[TriplePattern], count: Option[Long], graphEditor: GraphEditor[Any, Any]) {
    if (count.isDefined && count.get == 0) {
      // 0 intersection => no results => we're done.
      queryDone(graphEditor)
    } else {
      pairwisePatternCardinalities += ((patterns, count))
      if (pairwisePatternCardinalities.size == pairwisePatternsExpected) {
        allPairwiseStatisticsReceived = true
      }
    }
    //println(s"pairwise stats: ${pairwisePatternCardinalities.size}/$pairwisePatternsExpected")
  }

  def handleCardinalityReply(
    forPattern: TriplePattern,
    cardinality: Int,
    graphEditor: GraphEditor[Any, Any]) = {
    if (cardinality == 0) {
      // 0 cardinality => no results => we're done.
      queryDone(graphEditor)
    } else {
      cardinalities += forPattern -> cardinality
      receivedCardinalityReplies += 1
    }
    if (receivedCardinalityReplies == numberOfPatternsInOriginalQuery) {
      allCardinalitiesReceived = true
    }
  }

  def dispatchQuery(graphEditor: GraphEditor[Any, Any]) {
    if (dispatchedQuery.isDefined) {
      val q = dispatchedQuery.get
      graphEditor.sendSignal(q, q.routingAddress, None)
    } else {
      queryDone(graphEditor)
    }
  }

  def optimizeQuery: Option[Array[Int]] = {
    val optimizer = if (numberOfPatternsInOriginalQuery > 2) {
      new AdvancedOptimizer(pairwisePatternCardinalities)
    } else {
      GreedyCardinalityOptimizer
    }
    val optimizedPatterns = optimizer.optimize(cardinalities)
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

  def queryDone(graphEditor: GraphEditor[Any, Any]) {
    // Only execute this block once.
    if (!queryDone) {
      if (optimizingDuration == 0) {
        // We took a shortcut, did not await all cardinality/selectivity results.
        optimizingDuration = System.nanoTime - optimizingStartTime
      }
      resultPromise.success(state)
      val stats = Map[Any, Any](
        "isComplete" -> complete,
        "optimizingDuration" -> optimizingDuration,
        "queryCopyCount" -> queryCopyCount,
        "optimizedQuery" -> ("Pattern matching order: " + {
          if (dispatchedQuery.isDefined) {
            new QueryParticle(dispatchedQuery.get).patterns.toList
          } else { "the optimized was not run, probably one of the patterns had cardinality 0" }
        })).withDefaultValue("")
      statsPromise.success(stats)
      graphEditor.removeVertex(id)
      queryDone = true
    }
  }

}

case class ResultCount(patterns: Set[TriplePattern], count: Option[Long])

/**
 * If execution is complete returns Some(numberOfResults), else returns None.
 */
final class SimpleResultCountingQueryVertex(
  val requestor: Int,
  val querySpecification: QuerySpecification) extends BaseVertex[Int, Any, Long] {

  val id = QueryIds.nextCountQueryId
  val expectedTickets = querySpecification.tickets

  var receivedTickets = 0l
  var complete = true

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = 0
    val query = QueryParticle.fromSpecification(id, querySpecification)
    graphEditor.sendSignal(query, query.routingAddress, None)
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case tickets: Long =>
        processTickets(tickets)
        if (receivedTickets == expectedTickets) {
          queryDone(graphEditor)
        }
      case resultCount: Int =>
        state += resultCount
    }
    true
  }

  def processTickets(t: Long) {
    receivedTickets += math.abs(t)
    if (t < 0) {
      complete = false
    }
  }

  def queryDone(graphEditor: GraphEditor[Any, Any]) {
    val patterns = querySpecification.unmatched.toSet
    if (complete) {
      graphEditor.sendSignal(ResultCount(patterns, Some(state)), requestor, None)
    } else {
      graphEditor.sendSignal(ResultCount(patterns, None), requestor, None)
    }
    graphEditor.removeVertex(id)
  }

}
