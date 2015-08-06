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

abstract class AbstractQueryVertex[StateType](
    val query: Seq[TriplePattern],
    val tickets: Long,
    val numberOfSelectVariables: Int) extends BaseVertex[StateType] {

  lazy val queryTicketsReceived = new TicketSynchronization(s"queryTicketsReceived[${query.mkString}]", tickets, onFailure = None)

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
    signal match {
      case deliveredTickets: Long =>
        queryTicketsReceived.receivedTickets(deliveredTickets)
      case bindings: Array[_] =>
        handleBindings(bindings.asInstanceOf[Array[Array[Int]]])
      case resultCount: Int =>
        handleResultCount(resultCount)
    }
    true
  }

  def reportResultsAndRequestQueryVertexRemoval(complete: Boolean, graphEditor: GraphEditor[Long, Any]): Unit = {
    reportResults(complete)
    requestQueryVertexRemoval(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]])

  def handleResultCount(resultCount: Long)

  def requestQueryVertexRemoval(graphEditor: GraphEditor[Long, Any]): Unit = {
    graphEditor.removeVertex(id)
  }

  def reportResults(complete: Boolean): Unit

}
