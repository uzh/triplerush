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

package com.signalcollect.triplerush

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer

import com.signalcollect.GraphEditor
import com.signalcollect.ProcessingVertex

import akka.actor.ActorRef
import akka.actor.actorRef2Scala

object QueryOptimizer extends Enumeration with Serializable {

  val None = Value // Execute patterns in order passed.
  val Greedy = Value // Execute patterns in descending order of cardinalities.
  val Clever = Value // Uses cardinalities and prefers patterns that contain variables that have been matched in a previous pattern.

}

class QueryVertex(
    val query: PatternQuery,
    val resultRecipient: ActorRef,
    val optimizer: QueryOptimizer.Value) extends ProcessingVertex[Int, PatternQuery](query.queryId) {

  val expectedTickets: Long = query.tickets
  val expectedCardinalities = query.unmatched.size

  var receivedTickets: Long = 0
  var firstResultNanoTime = 0l
  var complete = true

  var optimizingStartTime = 0l
  var optimizingDuration = 0l

  var optimizedQuery: PatternQuery = _

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    if (optimizer != QueryOptimizer.None && query.unmatched.size > 1) {
      // Gather pattern cardinalities first.
      query.unmatched foreach (triplePattern => {
        val indexId = triplePattern.routingAddress
        optimizingStartTime = System.nanoTime
        graphEditor.sendSignal(CardinalityRequest(triplePattern, id), indexId, None)
      })
    } else {
      // Dispatch the query directly.
      graphEditor.sendSignal(query, query.unmatched.head.routingAddress, None)
      optimizedQuery = query
    }
  }

  var cardinalities: Map[TriplePattern, Int] = Map()

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case ticketsOfFailedQuery: Long =>
        receivedTickets += ticketsOfFailedQuery
//        println(s"Query vertex $id received tickets $ticketsOfFailedQuery. Now at $receivedTickets/$expectedTickets")
      case query: PatternQuery =>
        processQuery(query)
//        println(s"Query vertex $id received bindings ${query.bindings}. Now at $receivedTickets/$expectedTickets")
      case CardinalityReply(forPattern, cardinality) =>
        cardinalities += forPattern -> cardinality
//        println(s"Query vertex $id received cardinalities $forPattern -> $cardinality")
        if (cardinalities.size == expectedCardinalities) {
          optimizedQuery = optimizeQuery
          if (optimizingStartTime != 0) {
            optimizingDuration = System.nanoTime - optimizingStartTime
          }
          graphEditor.sendSignal(optimizedQuery, optimizedQuery.unmatched.head.routingAddress, None)
        }
    }
    true
  }

  def optimizeQuery: PatternQuery = {
    var sortedPatterns = cardinalities.toArray sortBy (_._2)
    optimizer match {
      case QueryOptimizer.Greedy =>
        // Sort triple patterns by cardinalities and send the query to the most selective pattern first.
        query.withUnmatchedPatterns(sortedPatterns map (_._1))
      case QueryOptimizer.Clever =>
        var boundVariables = Set[Int]() // The lower the score, the more constrained the variable.
        val optimizedPatterns = ArrayBuffer[TriplePattern]()
        while (!sortedPatterns.isEmpty) {
          val nextPattern = sortedPatterns.head._1
          optimizedPatterns.append(nextPattern)
          val variablesInPattern = nextPattern.variables
          for (variable <- variablesInPattern) {
            boundVariables += variable
          }
          sortedPatterns = sortedPatterns.tail map {
            case (pattern, oldCardinalityEstimate) =>
              // We don't care about the old estimate.
              var cardinalityEstimate = cardinalities(pattern).toDouble
              for (boundVariable <- boundVariables) {
                if (pattern.contains(boundVariable)) {
                  cardinalityEstimate = cardinalityEstimate / 100.0
                }
              }
              (pattern, cardinalityEstimate.toInt)
          }
          sortedPatterns = sortedPatterns sortBy (_._2)
        }
        query.withUnmatchedPatterns(optimizedPatterns.toArray)
    }
  }

  def processQuery(query: PatternQuery) {
    receivedTickets += query.tickets
    complete &&= query.isComplete
    //if (query.unmatched.isEmpty) {
    // numberOfSuccessfulQueries += 1
    // println(s"Success: $query")
    // Query was matched successfully.
    if (firstResultNanoTime == 0) {
      firstResultNanoTime = System.nanoTime
    }
    state = query :: state
    //} else {
    // numberOfFailedQueries += 1
    // println(s"Failure: $query")
    //}
  }

  override def scoreSignal: Double = if (expectedTickets == receivedTickets) 1 else 0

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    resultRecipient ! (state, Map("firstResultNanoTime" -> firstResultNanoTime, "optimizingDuration" -> optimizingDuration, "optimizedQuery" -> (optimizedQuery.toString + "\nCardinalities: " + cardinalities.toString)).withDefaultValue(""))
    //    val totalQueries = (numberOfFailedQueries + numberOfSuccessfulQueries).toDouble
    //    println(s"Total # of queries = $totalQueries failed : ${((numberOfFailedQueries / totalQueries) * 100.0).round}%")
    graphEditor.removeVertex(id)
  }

  def process(item: PatternQuery, graphEditor: GraphEditor[Any, Any]) {}

}