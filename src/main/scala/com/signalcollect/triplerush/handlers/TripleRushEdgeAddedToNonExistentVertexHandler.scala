/*
 *  @author Philip Stutz
 *
 *  Copyright 2014 University of Zurich
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

package com.signalcollect.triplerush.handlers

import com.signalcollect.{ Edge, GraphEditor, Vertex }
import com.signalcollect.interfaces.{ EdgeAddedToNonExistentVertexHandler, EdgeAddedToNonExistentVertexHandlerFactory }
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.vertices._
import com.signalcollect.triplerush._
import java.util.concurrent.atomic.AtomicInteger

case object TripleRushEdgeAddedToNonExistentVertexHandlerFactory extends EdgeAddedToNonExistentVertexHandlerFactory[Long, Any] {
  def createInstance: EdgeAddedToNonExistentVertexHandler[Long, Any] = TripleRushEdgeAddedToNonExistentVertexHandler

  override def toString = "TripleRushEdgeAddedToNonExistentVertexHandlerFactory"
}

case object TripleRushEdgeAddedToNonExistentVertexHandler extends EdgeAddedToNonExistentVertexHandler[Long, Any] {

  def handleImpossibleEdgeAddition(edge: Edge[Long], vertexId: Long, graphEditor: GraphEditor[Long, Any]): Option[Vertex[Long, _, Long, Any]] = {
    edge match {
      case b: BlockingIndexVertexEdge =>
        var remainingTickets = b.tickets
        val parentIds = IndexStructure.parentIds(vertexId)
        parentIds foreach { parentId =>
          val idDelta = vertexId.parentIdDelta(parentId)
          val indexType = IndexType(parentId)
          val tickets = IndexStructure.ticketsForIndexOperation(indexType)
          val oId = b.blockingOperationId
          remainingTickets -= tickets
          graphEditor.addEdge(parentId, new BlockingIndexVertexEdge(idDelta, tickets, oId))
        }
        assert(remainingTickets > 0)
        b.tickets = remainingTickets
    }
    IndexType(vertexId) match {
      case Root => Some(new RootIndex)
      case S    => Some(new SIndex(vertexId))
      case P    => Some(new PIndex(vertexId))
      case O    => Some(new OIndex(vertexId))
      case Sp   => Some(new SPIndex(vertexId))
      case So   => Some(new SOIndex(vertexId))
      case Po   => Some(new POIndex(vertexId))
    }
  }

}
