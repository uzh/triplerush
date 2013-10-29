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

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.UnrolledBuffer
import com.signalcollect.triplerush.QueryParticle._

import com.signalcollect.Edge
import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TriplePattern

import akka.actor.ActorRef
import akka.actor.actorRef2Scala

case class QueryDone(
  statKeys: Array[Any],
  statVariables: Array[Any])

case class QueryResult(
  bindings: UnrolledBuffer[Array[Int]],
  statKeys: Array[Any],
  statVariables: Array[Any])

case object QueryOptimizer {
  val None = 0 // Execute patterns in order passed.
  val Greedy = 1 // Execute patterns in descending order of cardinalities.
  val Clever = 2 // Uses cardinalities and prefers patterns that contain variables that have been matched in a previous pattern.
}

class QueryVertex(
  val query: Array[Int],
  val resultRecipientActor: ActorRef,
  val optimizer: Int,
  val recordStats: Boolean = false) extends Vertex[Int, Nothing] {

  val id = query.queryId

  @transient var state: Nothing = _

  val expectedTickets = Long.MaxValue
  val numberOfPatterns = query.numberOfPatterns

  @transient var queryCopyCount: Long = 0
  @transient var receivedTickets: Long = 0
  @transient var complete = true

  @transient var optimizingStartTime = 0l
  @transient var optimizingDuration = 0l

  @transient var optimizedQuery: Array[Int] = _

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    println(s"query vertex initialized for query ${query.queryId}")
    cardinalities = Map()
    if (optimizer != QueryOptimizer.None && numberOfPatterns > 1) {
      // Gather pattern cardinalities first.
      query.patterns foreach (triplePattern => {
        val responsibleIndexId = triplePattern.routingAddress
        optimizingStartTime = System.nanoTime
        println(s"Query @$id requests cardinality of $triplePattern")
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
      println(s"Query vertex $id has dispatched the query to ${query.routingAddress}.")
      graphEditor.sendSignal(query, query.routingAddress, None)
      optimizedQuery = query
    }
  }

  @transient var cardinalities: Map[TriplePattern, Int] = _

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]): Boolean = {
    signal match {
      case ticketsOfFailedQuery: Long =>
        queryCopyCount += 1
        processTickets(ticketsOfFailedQuery)
        if (receivedTickets == expectedTickets) {
          queryDone(graphEditor)
        }
      //        println(s"Query vertex $id received tickets $ticketsOfFailedQuery. Now at $receivedTickets/$expectedTickets")
      case bindings: Array[Array[Int]] =>
        queryCopyCount += 1
        resultRecipientActor ! bindings
      case CardinalityReply(forPattern, cardinality) =>
        handleCardinalityReply(forPattern, cardinality, graphEditor)
    }
    true
  }

  def handleCardinalityReply(
    forPattern: TriplePattern,
    cardinality: Int,
    graphEditor: GraphEditor[Any, Any]) = {
    println(s"query vertex @$id received cardinality for $forPattern ($cardinality)")
    if (cardinality == 0) {
      // 0 cardinality => no results => we're done.
      queryDone(graphEditor)
    } else {
      // TODO: If pattern is fully bound and cardinality is one, bind immediately.
      cardinalities += forPattern -> cardinality
      //        println(s"Query vertex $id received cardinalities $forPattern -> $cardinality")
      if (cardinalities.size == numberOfPatterns) {
        println(s"query vertex @$id has received all cardinalities")
        optimizedQuery = optimizeQuery
        if (optimizingStartTime != 0) {
          optimizingDuration = System.nanoTime - optimizingStartTime
        }
        graphEditor.sendSignal(optimizedQuery, optimizedQuery.routingAddress, None)
      }
    }
  }

  def optimizeQuery: Array[Int] = {
    println(s"query vertex @$id is optimizing the query")
    var sortedPatterns = cardinalities.toArray sortBy (_._2)
    optimizer match {
      case QueryOptimizer.Greedy =>
        // Sort triple patterns by cardinalities and send the query to the most selective pattern first.
        val copy = query.copy
        copy.writePatterns(sortedPatterns map (_._1))
        copy
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
              var foundUnbound = false
              for (variable <- pattern.variables) {
                if (boundVariables.contains(variable)) {
                  cardinalityEstimate = cardinalityEstimate / 100.0
                } else {
                  foundUnbound = true
                }
              }
              if (!foundUnbound) {
                cardinalityEstimate = 1.0 + cardinalityEstimate / 100000000
              }
              (pattern, cardinalityEstimate.toInt)
          }
          sortedPatterns = sortedPatterns sortBy (_._2)
        }
        val c = query.copy
        c.writePatterns(optimizedPatterns.toArray)
        c
    }
  }

  def processTickets(t: Long) {
    receivedTickets += math.abs(t)
    if (t < 0) {
      complete = false
    }
    println(s"Received tickets: $t, total of $receivedTickets")
    if (receivedTickets == Long.MaxValue) {
      println(s"RECEIVED ALL!")
    }
    //    //println((receivedTickets.toDouble / Long.MaxValue.toDouble).round + "%")
  }

  override def scoreSignal: Double = 0

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {}

  def queryDone(graphEditor: GraphEditor[Any, Any]) {
    if (recordStats) {
      val stats = Map[Any, Any](
        "optimizingDuration" -> optimizingDuration,
        "queryCopyCount" -> queryCopyCount,
        "optimizedQuery" -> (optimizedQuery.toString + "\nCardinalities: " + cardinalities.toString))
      val statsKeys: Array[Any] = stats.keys.toArray
      val statsValues: Array[Any] = (statsKeys map (key => stats(key))).toArray
      resultRecipientActor ! QueryDone(Array(), Array())
    } else {
      resultRecipientActor ! QueryDone(Array(), Array())
    }
    graphEditor.removeVertex(id)
  }

  def setState(s: Nothing) {
    state = s
  }

  def scoreCollect = 0 // Because signals are collected upon delivery.
  def edgeCount = 0
  override def toString = s"${this.getClass.getName}(state=$state)"
  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {}
  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}
  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = 0
}
