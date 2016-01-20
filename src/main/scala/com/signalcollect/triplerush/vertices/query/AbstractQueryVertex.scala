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

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush._
import com.signalcollect.triplerush.vertices.BaseVertex
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import akka.event.LoggingAdapter
import com.signalcollect.triplerush.dictionary.RdfDictionary

abstract class AbstractQueryVertex[StateType](
    query: Seq[TriplePattern],
    tickets: Long,
    numberOfSelectVariables: Int,
    dictionary: RdfDictionary,
    log: LoggingAdapter) extends BaseVertex[StateType] {

  lazy val queryTicketsReceived = new TicketSynchronization(s"queryTicketsReceived[${query.mkString}]", tickets, onFailure = None)

  var resultCount = 0
  val startTime = System.nanoTime
  val largeSearchSpaceSizeThreshold = 1000000
  var isLargeSearchSpaceReported = false

  // This value is only maintained if debug logging is enabled.
  var approximationOfExploredSearchSpace = 0

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]): Unit = {
    if (query.length > 0) {
      queryTicketsReceived.onComplete { complete =>
        reportResultsAndRequestQueryVertexRemoval(complete, graphEditor)
      }
      val particle = QueryParticle(
        patterns = query,
        queryId = OperationIds.extractFromLong(id),
        numberOfSelectVariables = numberOfSelectVariables,
        tickets = tickets)
      graphEditor.sendSignal(particle, particle.routingAddress)
    } else {
      // No patterns, no results: we can return results and remove the query vertex immediately.
      reportResultsAndRequestQueryVertexRemoval(complete = true, graphEditor)
    }
  }

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]): Boolean = {
    // `deliverSignalWithoutSourceId` is called a lot, we really want to be careful to not usually do anything expensive in here.
    if (log.isDebugEnabled) {
      approximationOfExploredSearchSpace += 1
      if (!isLargeSearchSpaceReported && approximationOfExploredSearchSpace > largeSearchSpaceSizeThreshold) {
        isLargeSearchSpaceReported = true
        val currentTime = System.nanoTime
        val deltaNanoseconds = currentTime - startTime
        val deltaMilliseconds = (deltaNanoseconds / 100000.0).round / 10.0
        log.debug(s"""
| Query has large search space and is still executing:
|   query = ${query.map(_.toDecodedString(dictionary)).mkString("[", ",\n", "]")}
|   current execution time = $deltaMilliseconds milliseconds
|   current results = $resultCount
|   current approximated size of explored search space = $approximationOfExploredSearchSpace
""".stripMargin)
      }
    }
    signal match {
      case deliveredTickets: Long =>
        queryTicketsReceived.receive(deliveredTickets)
      case bindings: Array[_] =>
        resultCount += 1
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        this.resultCount += resultCount
        handleResultCount(resultCount)
      case _ => false
    }
    true
  }

  def handleBindings(bindings: Array[Array[Int]]): Unit

  def handleResultCount(resultCount: Long): Unit

  def reportResults(complete: Boolean): Unit

  def reportResultsAndRequestQueryVertexRemoval(complete: Boolean, graphEditor: GraphEditor[Long, Any]): Unit = {
    reportResults(complete)
    requestQueryVertexRemoval(graphEditor)
  }

  def requestQueryVertexRemoval(graphEditor: GraphEditor[Long, Any]): Unit = {
    graphEditor.removeVertex(id)
  }

}
