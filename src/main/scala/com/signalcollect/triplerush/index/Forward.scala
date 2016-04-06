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

import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.IntSet
import com.signalcollect.triplerush.query.ParticleDebug
import com.signalcollect.triplerush.query.QueryParticle.arrayToParticle

import akka.actor.{ ActorSystem, actorRef2Scala }

class NotAForwardingIndex(message: String) extends Exception(message)

object Forward {

  // TODO: Allow for alternative index structures.
  def nextRoutingAddress(indexId: Long, childDelta: Int): Long = {
    // TODO: Make more efficient.
    val indexPattern = indexId.toTriplePattern
    IndexType(indexId) match {
      case Root => (indexPattern.copy(p = childDelta)).toEfficientIndexPattern
      case S    => indexPattern.copy(p = childDelta).toEfficientIndexPattern
      case P    => indexPattern.copy(s = childDelta).toEfficientIndexPattern
      case O    => indexPattern.copy(p = childDelta).toEfficientIndexPattern
      case other: Any => throw new NotAForwardingIndex(
        s"`Forward.nextRoutingAddress` was called from ${indexId.toTriplePattern}, but can only be called from a forwarding index vertex.")
    }
  }

  def forwardQuery(
    system: ActorSystem,
    indexId: Long,
    query: Array[Int],
    numberOfChildDeltas: Int,
    childDeltas: IntSet): Unit = {
    val edges = numberOfChildDeltas
    val totalTickets = query.tickets
    val absoluteValueOfTotalTickets = if (totalTickets < 0) -totalTickets else totalTickets // inlined math.abs
    val avg = absoluteValueOfTotalTickets / edges
    val complete = avg > 0 && totalTickets > 0
    var extras = absoluteValueOfTotalTickets % edges
    val averageTicketQuery = query.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = query.copyWithTickets(avg + 1, complete)
    val indexShard = Index.shard(system)
    def sendTo(childDelta: Int): Unit = {
      val routingAddress = nextRoutingAddress(indexId, childDelta)
      if (extras > 0) {
        extras -= 1
        indexShard ! aboveAverageTicketQuery
      } else if (avg > 0) {
        indexShard ! averageTicketQuery
      } else {
        // TODO: Send to query and optionally continue with more tickets.
        throw new OutOfTicketsException(
          s"Ran out of tickets for particle ${ParticleDebug(query).toString} at index ${indexId.toTriplePattern}.")
      }
    }
    childDeltas.foreach(sendTo)
  }

}
