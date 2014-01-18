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

final class ResultBindingQueryVertex(
  val querySpecification: QuerySpecification,
  val resultPromise: Promise[Traversable[Array[Int]]],
  val statsPromise: Promise[Map[Any, Any]],
  val optimizer: Option[Optimizer]) extends BaseVertex[Int, Any, ArrayOfArraysTraversable] {

  val query = querySpecification.toParticle
  val id = query.queryId

  val expectedTickets = query.tickets
  val numberOfPatterns = query.numberOfPatterns

  @transient var queryDone = false
  @transient var queryCopyCount: Long = 0
  @transient var receivedTickets: Long = 0
  @transient var complete = true

  @transient var optimizingStartTime = 0l
  @transient var optimizingDuration = 0l

  @transient var optimizedQuery: Array[Int] = _

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = new ArrayOfArraysTraversable
    cardinalities = Map()
    if (optimizer.isDefined && numberOfPatterns > 1) {
      // Gather pattern cardinalities.
      query.patterns foreach (triplePattern => {
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
      optimizedQuery = query
    } else {
      // Dispatch the query directly.
      graphEditor.sendSignal(query, query.routingAddress, None)
      optimizedQuery = query
    }
  }

  @transient var cardinalities: Map[TriplePattern, Int] = _

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
      // TODO: If pattern is fully bound and cardinality is one remove from query.
      cardinalities += forPattern -> cardinality
      if (cardinalities.size == numberOfPatterns) {
        optimizedQuery = optimizeQuery
        if (optimizingStartTime != 0) {
          optimizingDuration = System.nanoTime - optimizingStartTime
        }
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
      resultPromise.success(state)
      val stats = Map[Any, Any](
        "isComplete" -> complete,
        "optimizingDuration" -> optimizingDuration,
        "queryCopyCount" -> queryCopyCount,
        "optimizedQuery" -> ("Pattern matching order: " + new QueryParticle(optimizedQuery).
          patterns.toList + "\nCardinalities: " + cardinalities.toString)).
        withDefaultValue("")
      statsPromise.success(stats)
      graphEditor.removeVertex(id)
      queryDone = true
    }
  }

}
