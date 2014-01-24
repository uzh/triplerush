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

/**
 * If execution is complete returns Some(numberOfResults), else returns None.
 */
final class ResultCountingQueryVertex(
  val querySpecification: QuerySpecification,
  val resultPromise: Promise[Option[Long]],
  val optimizer: Option[Optimizer]) extends BaseVertex[Int, Any, Long] {

  val id = QueryIds.nextCountQueryId
  val expectedTickets = querySpecification.tickets
  val numberOfPatternsInOriginalQuery = querySpecification.unmatched.length
  var receivedCardinalityReplies = 0

  var queryDone = false
  var receivedTickets: Long = 0
  var complete = true

  var cardinalities: Map[TriplePattern, Long] = _
  var dispatchedQuery: Option[Array[Int]] = None

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = 0
    cardinalities = Map()
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
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
        processTickets(tickets)
        if (receivedTickets == expectedTickets) {
          queryDone(graphEditor)
        }
      case resultCount: Int =>
        state += resultCount
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
    println(s"@$id: $receivedTickets/$expectedTickets")
  }

  def queryDone(graphEditor: GraphEditor[Any, Any]) {
    // Only execute this block once.
    if (!queryDone) {
      if (complete) {
        resultPromise.success(Some(state))
      } else {
        resultPromise.success(None)
      }
      graphEditor.removeVertex(id)
      queryDone = true
    }
  }

}
