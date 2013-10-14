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

import com.signalcollect.interfaces.MessageBus
import com.signalcollect.messaging.SignalBulker
import com.signalcollect.interfaces.WorkerApiFactory
import com.signalcollect.messaging.AbstractMessageBus
import scala.reflect.ClassTag
import com.signalcollect.interfaces.VertexToWorkerMapper
import com.signalcollect.messaging.BulkMessageBus
import scala.collection.mutable.HashMap
import com.signalcollect.interfaces.SignalMessage
import com.signalcollect.interfaces.MessageBusFactory
import scala.collection.mutable.UnrolledBuffer
import QueryParticle._

class CombiningMessageBusFactory(flushThreshold: Int, withSourceIds: Boolean)
  extends MessageBusFactory {
  def createInstance[Id: ClassTag, Signal: ClassTag](
    numberOfWorkers: Int,
    numberOfNodes: Int,
    mapper: VertexToWorkerMapper[Id],
    sendCountIncrementorForRequests: MessageBus[_, _] => Unit,
    workerApiFactory: WorkerApiFactory): MessageBus[Id, Signal] = {
    new CombiningMessageBus[Id, Signal](
      numberOfWorkers,
      numberOfNodes,
      mapper,
      flushThreshold,
      withSourceIds,
      sendCountIncrementorForRequests: MessageBus[_, _] => Unit,
      workerApiFactory)
  }
  override def toString = "CombiningMessageBusFactory"
}

/**
 * Version of bulk message bus that combines tickets of failed queries.
 */
class CombiningMessageBus[Id: ClassTag, Signal: ClassTag](
  numberOfWorkers: Int,
  numberOfNodes: Int,
  mapper: VertexToWorkerMapper[Id],
  flushThreshold: Int,
  withSourceIds: Boolean,
  sendCountIncrementorForRequests: MessageBus[_, _] => Unit,
  workerApiFactory: WorkerApiFactory)
  extends BulkMessageBus[Id, Signal](numberOfWorkers,
    numberOfNodes,
    mapper,
    flushThreshold,
    withSourceIds,
    sendCountIncrementorForRequests,
    workerApiFactory) {

  val aggregatedTickets = new HashMap[Int, Long]().withDefaultValue(0)
  val aggregatedResults = new HashMap[Int, List[Array[Int]]]().withDefaultValue(null)

  override def sendSignal(
    signal: Signal,
    targetId: Id,
    sourceId: Option[Id],
    blocking: Boolean = false) {
    if (targetId.isInstanceOf[Int]) {
      val tId = targetId.asInstanceOf[Int]
      signal match {
        case tickets: Long => handleTickets(tickets, tId)
        case result: Array[Int] =>
          val oldResults = aggregatedResults(tId)
          val bindings = result.bindings
          handleTickets(result.tickets, tId)
          if (oldResults != null) {
            aggregatedResults(tId) = bindings :: oldResults
          } else {
            aggregatedResults(tId) = List(bindings)
          }
        case other =>
          super.sendSignal(signal, targetId, sourceId, blocking)
      }
    } else {
      // TODO: Also improve efficiency of sending non-result particles.
      super.sendSignal(signal, targetId, sourceId, blocking)
    }
  }

  private def handleTickets(tickets: Long, queryId: Int) {
    val oldTickets = aggregatedTickets(queryId)
    if (oldTickets < 0) {
      if (tickets < 0) {
        aggregatedTickets(queryId) = oldTickets + tickets
      } else {
        aggregatedTickets(queryId) = oldTickets - tickets
      }
    } else {
      if (tickets < 0) {
        aggregatedTickets(queryId) = tickets - oldTickets
      } else {
        aggregatedTickets(queryId) = tickets + oldTickets
      }
    }
  }

  override def flush {
    // It is important that the results arrive before the tickets, because
    // result tickets were separated from their respective results.
    if (!aggregatedResults.isEmpty) {
      for ((queryVertexId, results) <- aggregatedResults) {
        super.sendToWorkerForVertexId(
          SignalMessage(queryVertexId, null, results.toArray),
          queryVertexId.asInstanceOf[Id])
      }
      aggregatedResults.clear
    }
    if (!aggregatedTickets.isEmpty) {
      for ((queryVertexId, tickets) <- aggregatedTickets) {
        super.sendToWorkerForVertexId(
          SignalMessage(queryVertexId, null, tickets),
          queryVertexId.asInstanceOf[Id])
      }
      aggregatedTickets.clear
    }
    super.flush
  }

}
