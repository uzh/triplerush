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
import com.signalcollect.interfaces.MessageBusFactory
import QueryParticle._
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.util.IntLongHashMap
import com.signalcollect.util.IntHashMap
import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.util.IntIntHashMap
import akka.actor.ActorSystem
import com.signalcollect.interfaces.BulkSignal
import com.signalcollect.interfaces.BulkSignalNoSourceIds
import com.signalcollect.interfaces.SignalMessageWithoutSourceId
import com.signalcollect.triplerush.util.CompositeLongIntHashMap
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

class SignalBulkerWithoutIds[@specialized(Long) Id: ClassTag, Signal: ClassTag](size: Int) {
  private[this] var itemCount = 0

  def numberOfItems = itemCount

  def isFull: Boolean = itemCount == size

  final val targetIds = new Array[Id](size)
  final val signals = new Array[Signal](size)

  def addSignal(signal: Signal, targetId: Id): Unit = {
    signals(itemCount) = signal
    targetIds(itemCount) = targetId
    itemCount += 1
  }

  def clear(): Unit = {
    itemCount = 0
  }
}

class ResultBulker(val size: Int) {
  private[this] var itemCount = 0

  def numberOfItems = itemCount

  def isFull: Boolean = itemCount == size

  private[this] final val results = new Array[Array[Int]](size)

  def addResult(result: Array[Int]): Unit = {
    results(itemCount) = result
    itemCount += 1
  }

  def clear(): Unit = {
    itemCount = 0
  }

  def getResultArray: Array[Array[Int]] = {
    val resultsCopy = new Array[Array[Int]](itemCount)
    System.arraycopy(results, 0, resultsCopy, 0, itemCount)
    resultsCopy
  }
}

class CombiningMessageBusFactory[Signal: ClassTag](flushThreshold: Int, resultBulkerSize: Int)
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
      resultBulkerSize,
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
  val resultBulkerSize: Int,
  val sendCountIncrementorForRequests: MessageBus[_, _] => Unit,
  val workerApiFactory: WorkerApiFactory[Long, Signal])
    extends AbstractMessageBus[Long, Signal] {

  lazy val workerApi = workerApiFactory.createInstance(workerProxies, mapper)

  val aggregatedTickets = new IntLongHashMap(initialSize = 8)
  val aggregatedResults = new Int2ObjectOpenHashMap[ResultBulker](8)
  val aggregatedCardinalities = new CompositeLongIntHashMap(initialSize = 8)
  val aggregatedResultCounts = new IntIntHashMap(initialSize = 8)

  override def sendSignal(signal: Signal, targetId: Long): Unit = {
    // If message is sent to a Query Vertex
    if (targetId.isOperationId) {
      val extractedOperationId = OperationIds.extractFromLong(targetId)
      signal match {
        case resultCount: Int =>
          val oldResultCount = aggregatedResultCounts(extractedOperationId)
          aggregatedResultCounts(extractedOperationId) = oldResultCount + resultCount
        case tickets: Long =>
          handleTickets(tickets, extractedOperationId)
        case result: Array[Int] =>
          handleTickets(result.tickets, extractedOperationId)
          val bindings = result.bindings
          val oldResults = aggregatedResults.get(extractedOperationId)
          if (oldResults != null) {
            oldResults.addResult(bindings)
            if (oldResults.isFull) {
              val targetId = OperationIds.embedInLong(extractedOperationId)
              sendToWorkerForVertexId(SignalMessageWithoutSourceId(targetId, oldResults.getResultArray), targetId)
              aggregatedResults.remove(extractedOperationId)
            }
          } else {
            val newBulker = new ResultBulker(resultBulkerSize)
            newBulker.addResult(bindings)
            aggregatedResults.put(extractedOperationId, newBulker)
          }
        case other @ _ =>
          bulkSend(signal, targetId)
      }
    } // If message is sent to an Index Vertex
    else if (signal.isInstanceOf[Int]) {
      val t = targetId.asInstanceOf[Long]
      val oldCardinalities = aggregatedCardinalities(t)
      aggregatedCardinalities(t) = oldCardinalities + signal.asInstanceOf[Int]
    } else {
      bulkSend(signal, targetId)
    }
  }

  override def sendSignal(signal: Signal, targetId: Long, sourceId: Option[Long], blocking: Boolean = false): Unit = {
    throw new Exception(s"Non-optimized messaging for TripleRush, this should never be called: signal=$signal targetId=$targetId")
  }

  def bulkSend(
    signal: Signal,
    targetId: Long): Unit = {
    val workerId = mapper.getWorkerIdForVertexId(targetId)
    val bulker = outgoingMessages(workerId)
    bulker.addSignal(signal, targetId)
    pendingSignals += 1
    if (bulker.isFull) {
      pendingSignals -= bulker.numberOfItems
      // It seems that for small arrays arraycopy is faster than clone:
      // http://www.javaspecialists.co.za/archive/Issue124.html
      val length = bulker.signals.length
      val signalsClone = new Array[Signal](length)
      System.arraycopy(bulker.signals, 0, signalsClone, 0, length)
      val targetIdsClone = new Array[Long](length)
      System.arraycopy(bulker.targetIds, 0, targetIdsClone, 0, length)
      sendToWorker(workerId, BulkSignalNoSourceIds[Long, Signal](signalsClone, targetIdsClone))
      bulker.clear
    }
  }

  private[this] def handleTickets(tickets: Long, operationId: Int): Unit = {
    val oldTickets = aggregatedTickets(operationId)
    if (oldTickets < 0) {
      if (tickets < 0) {
        aggregatedTickets(operationId) = oldTickets + tickets
      } else {
        aggregatedTickets(operationId) = oldTickets - tickets
      }
    } else {
      if (tickets < 0) {
        aggregatedTickets(operationId) = tickets - oldTickets
      } else {
        aggregatedTickets(operationId) = tickets + oldTickets
      }
    }
  }

  override def flush(): Unit = {
    // It is important that the results arrive before the tickets, because
    // result tickets were separated from their respective results.
    if (!aggregatedResults.isEmpty) {
      val i = aggregatedResults.keySet().iterator()
      while (i.hasNext()) {
        val queryVertexId = i.nextInt()
        val results = aggregatedResults.get(queryVertexId)
        val targetId = OperationIds.embedInLong(queryVertexId)
        sendToWorkerForVertexId(SignalMessageWithoutSourceId(targetId, results.getResultArray), targetId)
      }
      aggregatedResults.clear()
    }
    if (!aggregatedResultCounts.isEmpty) {
      aggregatedResultCounts.process { (queryVertexId, resultCount) =>
        val targetId = OperationIds.embedInLong(queryVertexId)
        sendToWorkerForVertexId(SignalMessageWithoutSourceId(targetId, resultCount), targetId)
      }
    }
    if (!aggregatedTickets.isEmpty) {
      aggregatedTickets.process { (queryVertexId, tickets) =>
        val targetId = OperationIds.embedInLong(queryVertexId)
        sendToWorkerForVertexId(SignalMessageWithoutSourceId(targetId, tickets), targetId)
      }
    }
    if (!aggregatedCardinalities.isEmpty) {
      aggregatedCardinalities.process { (targetId, cardinalityIncrement) =>
        sendToWorkerForVertexId(SignalMessageWithoutSourceId(targetId, cardinalityIncrement), targetId)
      }
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
          sendToWorker(workerId, BulkSignalNoSourceIds[Long, Signal](
            signalsCopy,
            targetIdsCopy))
          outgoingMessages(workerId).clear
        }
        workerId += 1
      }
      pendingSignals = 0
    }
  }

  override def reset(): Unit = {
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

  val outgoingMessages: Array[SignalBulkerWithoutIds[Long, Signal]] = new Array[SignalBulkerWithoutIds[Long, Signal]](numberOfWorkers)
  for { i <- 0 until numberOfWorkers } {
    outgoingMessages(i) = new SignalBulkerWithoutIds[Long, Signal](flushThreshold)
  }

}
