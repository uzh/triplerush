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
  var edgeCounts = Map.empty[TriplePattern, Long]

  var dispatchedQuery: Option[Array[Int]] = None

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    println(s"Query vertex $id is executing query ${querySpecification.unmatched}")
    optimizingStartTime = System.nanoTime
    //TODO: Should we run an optimizer even for one-pattern queries?
    if (optimizer.isDefined && numberOfPatternsInOriginalQuery > 1) {
      // Gather pattern cardinalities.
      querySpecification.unmatched foreach (triplePattern => {
        val patternWithWildcards = triplePattern.withVariablesAsWildcards
        val fromCache = Cardinalities(patternWithWildcards)
        val pIndexForPattern = TriplePattern(0, patternWithWildcards.p, 0)
        // TODO: What if p is not bound, will this do the right thing?
        val edgeCountsCache = EdgeCounts(pIndexForPattern)
        if (fromCache.isDefined && edgeCountsCache.isDefined) {
          //caches defined
          handleCardinalityReply(triplePattern, fromCache.get, edgeCountsCache, graphEditor)
        } else {
          //at least one of the caches not defined
          val responsibleIndexId = patternWithWildcards.routingAddress
          println(s"responsible index id for $triplePattern is $responsibleIndexId")
          responsibleIndexId match {
            case root @ TriplePattern(0, 0, 0) =>
              handleCardinalityReply(triplePattern, Int.MaxValue, None, graphEditor)
            case other =>
              //sending cardinalityrequest to responsibleIndex
              println(s"Sending cardinality request for pattern $triplePattern to index vertex with id $responsibleIndexId")
              graphEditor.sendSignal(
                CardinalityRequest(triplePattern, id),
                responsibleIndexId, None)
              if (!edgeCountsCache.isDefined && responsibleIndexId != pIndexForPattern) {
                //also sending cardinality request for pIndex
                graphEditor.sendSignal(CardinalityRequest(triplePattern, id), pIndexForPattern, None)
              } else if (edgeCountsCache.isDefined) {
                //edgecount cache seems populated
                edgeCounts += pIndexForPattern -> edgeCountsCache.get
              }
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
        println(s"Query $id done because it has no patterns.")
        queryDone(graphEditor)
      }
    }
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case tickets: Long =>
        processTickets(tickets)
        if (receivedTickets == expectedTickets) {
          println(s"Query $id done because it received all tickets")
          queryDone(graphEditor)
        }
      case bindings: Array[_] =>
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        // TODO, Wrap the msg and also make this a long.
        handleResultCount(resultCount)
      case CardinalityReply(forPattern, cardinality) =>
        //received cardinality reply from: forPattern
        handleCardinalityReply(forPattern, cardinality, None, graphEditor)
      case CardinalityAndEdgeCountReply(forPattern, cardinality, edgeCount) =>
        //received cardinalityandedgecount reply from: forPattern
        handleCardinalityReply(forPattern, cardinality, Some(edgeCount), graphEditor)
    }
    true
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)
  
  def handleCardinalityReply(
    forPattern: TriplePattern,
    cardinality: Long,
    edgeCountOption: Option[Long],
    graphEditor: GraphEditor[Any, Any]) = {

    if(!forPattern.isValidQueryPattern){
    	throw new Exception(s"Query Pattern: $forPattern is not valid and contains wildcards.")
    }
    
    if (edgeCountOption.isDefined) {
      // TODO(Bibek): Make more elegant. 
      val edgeCount = edgeCountOption.get
      val pIndexForPattern = TriplePattern(0, forPattern.p, 0)
      edgeCounts += pIndexForPattern -> edgeCount
      EdgeCounts.add(pIndexForPattern, edgeCount)
    }

    cardinalities += forPattern -> cardinality
    Cardinalities.add(forPattern.withVariablesAsWildcards, cardinality)
    if (cardinality == 0) {
      // 0 cardinality => no results => we're done.
      println(s"Query $id done because it received cardinality 0 for pattern $forPattern")
      queryDone(graphEditor)
    } else {
      gatheredCardinalities += 1
      if (!isQueryDone && gatheredCardinalities == numberOfPatternsInOriginalQuery) {
        dispatchedQuery = optimizeQuery
        if (dispatchedQuery.isDefined) {
          graphEditor.sendSignal(dispatchedQuery.get, dispatchedQuery.get.routingAddress, None)
        } else {
          println("Query done because the optimizer got rid of it.")
          queryDone(graphEditor)
        }
      }
    }
  }

  def optimizeQuery: Option[Array[Int]] = {
    val optimizedPatterns = optimizer.get.optimize(cardinalities, Some(edgeCounts))
    optimizingDuration = System.nanoTime - optimizingStartTime
    if (optimizedPatterns.length > 0) {
      println(s"optimized patterns from query vertex $id: ${optimizedPatterns.toList}")
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
