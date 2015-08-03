/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 iHealth Technologies
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

class BlockingTripleAdditionsVertex(
    val triples: Iterator[TriplePattern],
    val batchSize: Int = 10000) extends BaseVertex[Option[TicketSynchronization]] {

  setState(None)

  val id = OperationIds.embedInLong(OperationIds.nextId)

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]): Unit = {
    // TODO: Dispatch a batch of triple additions and setup synchronization.
    ???
  }

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]): Boolean = {
    signal match {
      case deliveredTickets: Long =>
        state match {
          case None =>
            throw new Exception(
              s"Blocking triple addition vertex received tickets when there was no ongoing synchrinization.")
          case Some(s) =>
            s.receivedTickets(deliveredTickets)
        }
      case other: Any =>
        throw new UnsupportedOperationException(
          s"Blocking triple addition vertex received an unsupported message $signal of type ${signal.getClass.getSimpleName}.")
    }
    true
  }

}
