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

import scala.util.Random
import com.signalcollect.Edge
import com.signalcollect.GraphEditor

object SignalSet extends Enumeration with Serializable {
  val BoundSubject = Value
  val BoundPredicate = Value
  val BoundObject = Value
}

class IndexVertex(id: TriplePattern) extends PatternVertex(id) {

  override def edgeCount = targetIds.length

  var targetIds = List[TriplePattern]()

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val targetId = e.targetId.asInstanceOf[TriplePattern]
    targetIds = targetId :: targetIds
    true
  }

  def processSamplingQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val targetIdCount = targetIds.length
    val bins = new Array[Long](targetIdCount)
    for (i <- 1l to query.tickets) {
      val randomIndex = Random.nextInt(targetIdCount)
      bins(randomIndex) += 1
    }
    val complete: Boolean = bins forall (_ > 0)
    var binIndex = 0
    for (targetId <- targetIds) {
      val ticketsForEdge = bins(binIndex)
      if (ticketsForEdge > 0) {
        val ticketEquippedQuery = query.withTickets(ticketsForEdge, complete)
        graphEditor.sendSignal(ticketEquippedQuery, targetId, None)
      }
      binIndex += 1
    }
  }

  def processFullQuery(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val targetIdCount = targetIds.length
    val avg = query.tickets / targetIdCount
    val complete = avg > 0
    var extras = query.tickets % targetIdCount
    val averageTicketQuery = query.withTickets(avg, complete)
    val aboveAverageTicketQuery = query.withTickets(avg + 1, complete)
    for (targetId <- targetIds) {
      if (extras > 0) {
        graphEditor.sendSignal(aboveAverageTicketQuery, targetId, None)
        extras -= 1
      } else if (complete) {
        graphEditor.sendSignal(averageTicketQuery, targetId, None)
      }
    }
  }

  override def process(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    if (query.isSamplingQuery) {
      processSamplingQuery(query, graphEditor)
    } else {
      processFullQuery(query, graphEditor)
    }
  }
}