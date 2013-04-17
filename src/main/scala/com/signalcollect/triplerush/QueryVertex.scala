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

import com.signalcollect._
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

class QueryVertex(
    val query: PatternQuery,
    val promise: Promise[(List[PatternQuery], Map[String, Any])]) extends ProcessingVertex[Int, PatternQuery](query.queryId) {

  val expectedTickets: Long = query.tickets

  var receivedTickets: Long = 0
  var firstResultNanoTime = 0l
  var complete = true

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    graphEditor.sendSignal(query, query.unmatched.head.routingAddress, None)
  }

  var cardinalities: Map[TriplePattern, Int] = Map()
  
  override def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    signal match {
      case query: PatternQuery =>
        processQuery(query)
      case CardinalityReply(forPattern, cardinality) =>
        cardinalities += forPattern -> cardinality
    }
    true
  }

  //  var numberOfFailedQueries = 0
  //  var numberOfSuccessfulQueries = 0

  def processQuery(query: PatternQuery) {
    receivedTickets += query.tickets
    complete &&= query.isComplete
    if (query.unmatched.isEmpty) {
      // numberOfSuccessfulQueries += 1
      // println(s"Success: $query")
      // Query was matched successfully.
      if (firstResultNanoTime == 0) {
        firstResultNanoTime = System.nanoTime
      }
      state = query :: state
    } else {
      // numberOfFailedQueries += 1
      // println(s"Failure: $query")
    }
  }

  override def scoreSignal: Double = if (expectedTickets == receivedTickets) 1 else 0

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    promise success (state, Map("firstResultNanoTime" -> firstResultNanoTime))
    //    val totalQueries = (numberOfFailedQueries + numberOfSuccessfulQueries).toDouble
    //    println(s"Total # of queries = $totalQueries failed : ${((numberOfFailedQueries / totalQueries) * 100.0).round}%")
    graphEditor.removeVertex(id)
  }

  def process(item: PatternQuery, graphEditor: GraphEditor[Any, Any]) {}

}