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
import com.signalcollect.interfaces.SignalMessage
import com.signalcollect.interfaces.MessageBusFactory
import QueryParticle._
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.util.IntLongHashMap
import com.signalcollect.util.IntHashMap
import com.signalcollect.triplerush.util.TriplePatternIntHashMap
import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.util.IntIntHashMap
import akka.actor.ActorSystem
import com.signalcollect.interfaces.BulkSignal
import com.signalcollect.interfaces.BulkSignalNoSourceIds

class CombiningMessageBusFactory[Signal: ClassTag](flushThreshold: Int, withSourceIds: Boolean)
  extends MessageBusFactory[Long, Signal] {
  def createInstance(
    system: ActorSystem,
    numberOfWorkers: Int,
    numberOfNodes: Int,
    mapper: VertexToWorkerMapper[Long],
    sendCountIncrementorForRequests: MessageBus[_, _] => Unit,
    workerApiFactory: WorkerApiFactory[Long, Signal]): MessageBus[Long, Signal] = {
    new CombiningMessageBus[Signal](
      system,
      numberOfWorkers,
      numberOfNodes,
      mapper.asInstanceOf[VertexToWorkerMapper[Long]],
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
final class CombiningMessageBus[Signal: ClassTag](
  val system: ActorSystem,
  val numberOfWorkers: Int,
  val numberOfNodes: Int,
  val mapper: VertexToWorkerMapper[Long],
  val flushThreshold: Int,
  val withSourceIds: Boolean,
  val sendCountIncrementorForRequests: MessageBus[_, _] => Unit,
  val workerApiFactory: WorkerApiFactory[Long, Signal])
  extends AbstractMessageBus[Long, Signal] {

  lazy val workerApi = workerApiFactory.createInstance(workerProxies, mapper)

  val aggregatedTickets = new IntLongHashMap(initialSize = 8)
  val aggregatedResults = new IntHashMap[ArrayBuffer[Array[Int]]](initialSize = 8)
  val aggregatedCardinalities = new TriplePatternIntHashMap(initialSize = 8)
  val aggregatedResultCounts = new IntIntHashMap(initialSize = 8)

  override def sendSignal(signal: Signal, targetId: Long, sourceId: Option[Long]) {
    sendSignal(signal, targetId, sourceId, false)
  }

  // Ignores blocking.
  @inline override def sendSignal(
    signal: Signal,
    targetId: Long,
    sourceId: Option[Long],
    blocking: Boolean = false) {
    // If message is sent to a Query Vertex 
    if (targetId.isQueryId) {
      val extractedQueryId = QueryIds.extractQueryIdFromLong(targetId)
      signal match {
        case resultCount: Int =>
          val oldResultCount = aggregatedResultCounts(extractedQueryId)
          aggregatedResultCounts(extractedQueryId) = oldResultCount + resultCount
        case tickets: Long =>
          handleTickets(tickets, extractedQueryId)
        case result: Array[Int] =>
          val oldResults = aggregatedResults(extractedQueryId)
          val bindings = result.bindings
          handleTickets(result.tickets, extractedQueryId)
          if (oldResults != null) {
            oldResults.append(bindings)
          } else {
            val newBuffer = ArrayBuffer(bindings)
            aggregatedResults(extractedQueryId) = newBuffer
          }
        case other =>
          bulkSend(signal, targetId, sourceId)
      }
    } // If message is sent to an Index Vertex
    else if (signal.isInstanceOf[Int]) {
      val t = targetId.asInstanceOf[Long]
      val oldCardinalities = aggregatedCardinalities(t)
      aggregatedCardinalities(t) = oldCardinalities + signal.asInstanceOf[Int]
    } else {
      // TODO: Also improve efficiency of sending non-result particles. Compression?
      bulkSend(signal, targetId, sourceId)
    }
  }

  def bulkSend(
    signal: Signal,
    targetId: Long,
    sourceId: Option[Long],
    blocking: Boolean = false) {
    val workerId = mapper.getWorkerIdForVertexId(targetId)
    val bulker = outgoingMessages(workerId)
    if (withSourceIds) {
      bulker.addSignal(signal, targetId, sourceId)
    } else {
      bulker.addSignal(signal, targetId, None)
    }
    pendingSignals += 1
    if (bulker.isFull) {
      pendingSignals -= bulker.numberOfItems
      if (withSourceIds) {
        super.sendToWorker(workerId, BulkSignal[Long, Signal](bulker.signals.clone, bulker.targetIds.clone, bulker.sourceIds.clone))
      } else {
        super.sendToWorker(workerId, BulkSignalNoSourceIds[Long, Signal](bulker.signals.clone, bulker.targetIds.clone))
      }
      bulker.clear
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
      aggregatedResults.foreach { (queryVertexId, results) =>
        super.sendSignal(results.toArray.asInstanceOf[Signal], QueryIds.embedQueryIdInLong(queryVertexId), null, false)
      }
      aggregatedResults.clear
    }
    if (!aggregatedResultCounts.isEmpty) {
      aggregatedResultCounts.foreach { (queryVertexId, resultCount) =>
        super.sendSignal(resultCount.asInstanceOf[Signal], QueryIds.embedQueryIdInLong(queryVertexId), null, false)
      }
      aggregatedResultCounts.clear
    }
    if (!aggregatedTickets.isEmpty) {
      aggregatedTickets.foreach { (queryVertexId, tickets) =>
        super.sendSignal(tickets.asInstanceOf[Signal], QueryIds.embedQueryIdInLong(queryVertexId), null, false)
      }
      aggregatedTickets.clear
    }
    if (!aggregatedCardinalities.isEmpty) {
      aggregatedCardinalities.foreach { (targetId, cardinalityIncrement) =>
        super.sendSignal(cardinalityIncrement.asInstanceOf[Signal], targetId, null, false)
      }
      aggregatedCardinalities.clear
    }
    if (pendingSignals > 0) {
      var workerId = 0
      while (workerId < numberOfWorkers) {
        val bulker = outgoingMessages(workerId)
        val signalCount = bulker.numberOfItems
        if (signalCount > 0) {
          val signalsCopy = new Array[Signal](signalCount)
          System.arraycopy(bulker.signals, 0, signalsCopy, 0, signalCount)
          val targetIdsCopy = new Array[Long](signalCount)
          System.arraycopy(bulker.targetIds, 0, targetIdsCopy, 0, signalCount)
          if (withSourceIds) {
            val sourceIdsCopy = new Array[Long](signalCount)
            System.arraycopy(bulker.sourceIds, 0, sourceIdsCopy, 0, signalCount)
            super.sendToWorker(workerId, BulkSignal[Long, Signal](
              signalsCopy,
              targetIdsCopy,
              sourceIdsCopy))
          } else {
            super.sendToWorker(workerId, BulkSignalNoSourceIds[Long, Signal](
              signalsCopy,
              targetIdsCopy))
          }
          outgoingMessages(workerId).clear
        }
        workerId += 1
      }
      pendingSignals = 0
    }
  }

  override def reset {
    super.reset
    pendingSignals = 0
    var i = 0
    val bulkers = outgoingMessages.length
    while (i < bulkers) {
      outgoingMessages(i).clear
      i += 1
    }
  }

  protected var pendingSignals = 0

  val outgoingMessages: Array[SignalBulker[Long, Signal]] = new Array[SignalBulker[Long, Signal]](numberOfWorkers)
  for (i <- 0 until numberOfWorkers) {
    outgoingMessages(i) = new SignalBulker[Long, Signal](flushThreshold)
  }

}
