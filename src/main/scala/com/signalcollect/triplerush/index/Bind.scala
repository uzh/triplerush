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

import akka.actor.ActorSystem
import com.signalcollect.triplerush.query.OperationIds
import com.signalcollect.triplerush.IntSet
import com.signalcollect.triplerush.query.QueryParticle
import com.signalcollect.triplerush.query.QueryParticle._
import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.result.QueryExecutionHandler
import akka.actor.ActorRef
import com.signalcollect.triplerush.query.ParticleDebug

class NotABindingIndex(message: String) extends Exception(message)

object Bind {

  def bindQuery(
    system: ActorSystem,
    indexId: Long,
    query: Array[Int],
    numberOfChildDeltas: Int,
    childDeltas: IntSet): Unit = {
    val totalTickets = query.tickets
    val absoluteValueOfTotalTickets = if (totalTickets < 0) -totalTickets else totalTickets // inlined math.abs
    val avg = absoluteValueOfTotalTickets / numberOfChildDeltas
    val complete = avg > 0 && totalTickets > 0
    var extras = absoluteValueOfTotalTickets % numberOfChildDeltas
    val averageTicketQuery = query.copyWithTickets(avg, complete)
    val aboveAverageTicketQuery = query.copyWithTickets(avg + 1, complete)
    def bind(childDelta: Int): Unit = {
      if (extras > 0) {
        extras -= 1
        handleQueryBinding(system, indexId, childDelta, aboveAverageTicketQuery)
      } else if (avg > 0) {
        handleQueryBinding(system, indexId, childDelta, averageTicketQuery)
      }
    }
    childDeltas.foreach(bind)
  }

  def handleQueryBinding(
    system: ActorSystem,
    indexId: Long,
    childDelta: Int,
    query: Array[Int]): Unit = {
    val boundParticle = bindIndividualQuery(indexId, childDelta, query)
    if (boundParticle != null) {
      if (boundParticle.isResult) {
        // Query successful, send to query vertex.
        val query = QueryExecutionHandler.shard(system)
        query ! QueryExecutionHandler.BindingsForQuery(boundParticle.queryId, boundParticle.bindings)
        query ! QueryExecutionHandler.Tickets(boundParticle.queryId, boundParticle.tickets)
      } else {
        // TODO: Handle existence checks.
        assert(!boundParticle.lastPattern.isFullyBound,
          s"Triple existence checks required for ${ParticleDebug(boundParticle)}, but not implemented yet.")
        Index.shard(system) ! boundParticle
      }
    } else {
      // Failed to bind, send to query vertex.
      QueryExecutionHandler.shard(system) ! query.tickets
    }
  }

  def bindIndividualQuery(indexId: Long, childDelta: Int, query: Array[Int]): Array[Int] = {
    val indexPattern = indexId.toTriplePattern
    IndexType(indexId) match {
      case Sp => query.bind(indexPattern.s, indexPattern.p, childDelta)
      case So => query.bind(indexPattern.s, childDelta, indexPattern.o)
      case Po => query.bind(childDelta, indexPattern.p, indexPattern.o)
      case other: Any => throw new NotABindingIndex(
        s"`Bind.bindIndividualQuery` was called from ${indexPattern}, but can only be called from a binding index vertex.")
    }
  }

}
