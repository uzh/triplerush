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

final class ResultBindingQueryVertex(
  val querySpecification: QuerySpecification,
  val resultPromise: Promise[Traversable[Array[Int]]],
  val statsPromise: Promise[Map[Any, Any]],
  val optimizer: Option[Optimizer]) extends BaseVertex[Int, Any, ArrayOfArraysTraversable] {

  val id = QueryIds.nextQueryId

  val expectedTickets = querySpecification.tickets
  val numberOfPatternsInOriginalQuery = querySpecification.unmatched.length
  var receivedCardinalityReplies = 0
  var queryDone = false
  var queryCopyCount: Long = 0
  var receivedTickets: Long = 0
  var complete = true

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  var cardinalities: Map[TriplePattern, Int] = _
  var dispatchedQuery: Option[Array[Int]] = None

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = new ArrayOfArraysTraversable
    cardinalities = Map()
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
      // Gather pattern cardinalities.
      querySpecification.unmatched foreach (triplePattern => {
        val responsibleIndexId = triplePattern.routingAddress
        optimizingStartTime = System.nanoTime
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
      if (numberOfPatternsInOriginalQuery > 0) {
        dispatchedQuery = Some(QueryParticle.fromSpecification(id, querySpecification))
        graphEditor.sendSignal(dispatchedQuery.get, dispatchedQuery.get.routingAddress, None)
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
      case CardinalityReply(forPattern, cardinality) =>
        handleCardinalityReply(forPattern, cardinality, graphEditor)
    }
    true
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
      if (receivedCardinalityReplies == numberOfPatternsInOriginalQuery) {
        dispatchedQuery = optimizeQuery
        if (optimizingStartTime != 0) {
          optimizingDuration = System.nanoTime - optimizingStartTime
        }
        if (dispatchedQuery.isDefined) {
          graphEditor.sendSignal(dispatchedQuery.get, dispatchedQuery.get.routingAddress, None)
        } else {
          queryDone(graphEditor)
        }
      }
    }
  }

  def optimizeQuery: Option[Array[Int]] = {
    val optimizedPatterns = optimizer.get.optimize(cardinalities)
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
      resultPromise.success(state)
      val stats = Map[Any, Any](
        "isComplete" -> complete,
        "optimizingDuration" -> optimizingDuration,
        "queryCopyCount" -> queryCopyCount,
        "optimizedQuery" -> ("Pattern matching order: " + {
          if (dispatchedQuery.isDefined) {
            new QueryParticle(dispatchedQuery.get).patterns.toList + "\nCardinalities: " + cardinalities.toString
          } else { "the optimized was not run, probably one of the patterns had cardinality 0" }
        })).withDefaultValue("")
      statsPromise.success(stats)
      graphEditor.removeVertex(id)
      queryDone = true
    }
  }

}
