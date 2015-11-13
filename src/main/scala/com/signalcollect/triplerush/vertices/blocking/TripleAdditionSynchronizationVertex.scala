/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 Cotiviti
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

package com.signalcollect.triplerush.vertices.blocking

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.vertices.BaseVertex
import scala.concurrent.Promise
import com.signalcollect.triplerush.vertices.query.TicketSynchronization
import com.signalcollect.triplerush.BlockingIndexVertexEdge
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.IndexStructure
import com.signalcollect.triplerush.OperationIds
import scala.util.Try
import scala.util.Failure
import scala.util.Success

class TripleAdditionSynchronizationVertex(
    triples: Iterator[TriplePattern],
    operationCompletedPromise: Promise[Unit],
    batchSize: Int = 10000) extends BaseVertex[Option[TicketSynchronization]] {

  val operationId = OperationIds.nextId
  val id = OperationIds.embedInLong(operationId)

  /**
   * Propagates an error in the passed code block to `operationCompletedPromise`.
   */
  private[this] def propagateError(graphEditor: GraphEditor[Long, Any])(f: => Unit): Unit = {
    val operationAttempt = Try(f)
    operationAttempt match {
      case Failure(f) =>
        operationCompletedPromise.failure(f)
        graphEditor.removeVertex(id)
      case Success(()) =>
    }
  }

  def dispatchTripleAdditionBatch(graphEditor: GraphEditor[Long, Any]): Unit = {
    var dispatchedTriples = 0L
    while (triples.hasNext && dispatchedTriples < batchSize) {
      dispatchedTriples += 1
      val t = triples.next
      val s = t.s
      val p = t.p
      val o = t.o
      val po = EfficientIndexPattern(0, p, o)
      val so = EfficientIndexPattern(s, 0, o)
      val sp = EfficientIndexPattern(s, p, 0)
      graphEditor.addEdge(po, new BlockingIndexVertexEdge(s, IndexStructure.ticketsForIndexOperation(po), operationId))
      graphEditor.addEdge(so, new BlockingIndexVertexEdge(p, IndexStructure.ticketsForIndexOperation(so), operationId))
      graphEditor.addEdge(sp, new BlockingIndexVertexEdge(o, IndexStructure.ticketsForIndexOperation(sp), operationId))
    }
    val expectedTickets = dispatchedTriples * IndexStructure.ticketsForTripleOperation
    val synchronization = new TicketSynchronization("BlockingTripleAdditionsVertex", expectedTickets)
    synchronization.onSuccess {
      propagateError(graphEditor) {
        if (triples.hasNext) {
          dispatchTripleAdditionBatch(graphEditor)
        } else {
          operationCompletedPromise.success(())
          graphEditor.removeVertex(id)
        }
      }
    }
    setState(Some(synchronization))
    synchronization.receive(0) // Immediately complete synchronization if the iterator was empty.
  }

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]): Unit = {
    propagateError(graphEditor) {
      dispatchTripleAdditionBatch(graphEditor)
    }
  }

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]): Boolean = {
    propagateError(graphEditor) {
      signal match {
        case deliveredTickets: Long =>
          state match {
            case None =>
              val msg = s"Blocking triple addition vertex received tickets when there was no ongoing synchronization."
              throw new Error(msg)
            case Some(s) =>
              s.receive(deliveredTickets)
          }
        case other @ _ =>
          throw new UnsupportedOperationException(
            s"Blocking triple addition vertex received an unsupported message $signal of type ${signal.getClass.getSimpleName}.")
      }
    }
    true
  }

}
