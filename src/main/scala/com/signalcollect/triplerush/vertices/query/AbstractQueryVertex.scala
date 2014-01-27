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

abstract class AbstractQueryVertex[StateType](
  val querySpecification: QuerySpecification,
  val optimizer: Option[Optimizer]) extends BaseVertex[Int, Any, StateType] {

  val expectedTickets = querySpecification.tickets
  val numberOfPatternsInOriginalQuery = querySpecification.unmatched.length

  var gatheredCardinalities = 0
  var isQueryDone = false
  var receivedTickets = 0l
  var complete = true

  var cardinalities = Map.empty[TriplePattern, Long]
  var dispatchedQuery: Option[Array[Int]] = None

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    optimizingStartTime = System.nanoTime
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
      // Gather pattern cardinalities.
      querySpecification.unmatched foreach (triplePattern => {
        val fromCache = Cardinalities(triplePattern)
        if (fromCache.isDefined) {
          handleCardinalityReply(triplePattern, fromCache.get, graphEditor)
        } else {
          val responsibleIndexId = triplePattern.routingAddress
          responsibleIndexId match {
            case root @ TriplePattern(0, 0, 0) =>
              handleCardinalityReply(triplePattern, Int.MaxValue, graphEditor)
            case other =>
              graphEditor.sendSignal(
                CardinalityRequest(triplePattern, id),
                responsibleIndexId, None)
          }
        }
      })
    } else {
      // Dispatch the query directly.
      optimizingDuration = System.nanoTime - optimizingStartTime
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
      case bindings: Array[_] =>
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        // TODO, Wrap the msg and also make this a long.
        handleResultCount(resultCount)
      case CardinalityReply(forPattern, cardinality) =>
        handleCardinalityReply(forPattern, cardinality, graphEditor)
    }
    true
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)

  def handleCardinalityReply(
    forPattern: TriplePattern,
    cardinality: Long,
    graphEditor: GraphEditor[Any, Any]) = {
    if (cardinality == 0) {
      // 0 cardinality => no results => we're done.
      queryDone(graphEditor)
    } else {
      cardinalities += forPattern -> cardinality
      gatheredCardinalities += 1
      if (gatheredCardinalities == numberOfPatternsInOriginalQuery) {
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

  def queryDone(graphEditor: GraphEditor[Any, Any]) {
    if (!isQueryDone) {
      isQueryDone = true
      graphEditor.removeVertex(id)
    }
  }

}
