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
import com.signalcollect.triplerush.Optimizer
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable

/**
 * If execution is complete returns Some(numberOfResults), else returns None.
 */
final class ResultCountingQueryVertex(
  val querySpecification: QuerySpecification,
  val resultPromise: Promise[Option[Int]],
  val optimizer: Option[Optimizer]) extends BaseVertex[Int, Any, Int] {

  val query = QueryParticle.fromSpecification(querySpecification, withBindings = false)
  println(s"id = ${new QueryParticle(query).queryId}")
  val id = query.queryId

  val expectedTickets = query.tickets
  val numberOfPatterns = query.numberOfPatterns

  @transient var queryDone = false
  @transient var receivedTickets: Long = 0
  @transient var complete = true

  @transient var optimizedQuery: Array[Int] = _

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = 0
    cardinalities = Map()
    if (optimizer.isDefined && numberOfPatterns > 1) {
      // Gather pattern cardinalities.
      query.patterns foreach (triplePattern => {
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
      graphEditor.sendSignal(query, query.routingAddress, None)
    }
  }

  @transient var cardinalities: Map[TriplePattern, Int] = _

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
      if (cardinalities.size == numberOfPatterns) {
        optimizedQuery = optimizeQuery
        graphEditor.sendSignal(optimizedQuery, optimizedQuery.routingAddress, None)
      }
    }
  }

  def optimizeQuery: Array[Int] = {
    val c = query.copy
    if (optimizer.isEmpty) {
      c
    } else {
      val optimizedPatterns = optimizer.get.optimize(cardinalities)
      c.writePatterns(optimizedPatterns)
      c
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
