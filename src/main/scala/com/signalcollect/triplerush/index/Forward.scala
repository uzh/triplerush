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

package com.signalcollect.triplerush.index

import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.triplerush.query.QueryParticle
import com.signalcollect.triplerush.query.QueryParticle._

object Forward {

  def nextRoutingAddress(indexId: Long, childDelta: Int): Long = ???

  def forwardQuery(
    indexId: Long,
    query: Array[Int],
    numberOfChildDeltas: Int,
    childDeltas: Traversable[Int]): Unit = {
    val edges = numberOfChildDeltas
    val totalTickets = query.tickets
    val absoluteValueOfTotalTickets = if (totalTickets < 0) -totalTickets else totalTickets // inlined math.abs
    val avg = absoluteValueOfTotalTickets / edges
    val complete = avg > 0 && totalTickets > 0
    var extras = absoluteValueOfTotalTickets % edges
    val averageTicketQuery = query.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = query.copyWithTickets(avg + 1, complete)
    def sendTo(childDelta: Int): Unit = {
      val routingAddress = nextRoutingAddress(indexId, childDelta)
      if (extras > 0) {
        extras -= 1
        //graphEditor.sendSignal(aboveAverageTicketQuery, routingAddress)
        //TODO: Replace with code that sends to index actor.
      } else if (avg > 0) {
        //graphEditor.sendSignal(averageTicketQuery, routingAddress)
        //TODO: Replace with code that sends to index actor.
      }
    }
    childDeltas.foreach(sendTo)
  }

}
