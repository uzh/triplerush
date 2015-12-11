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

import com.signalcollect.GraphEditor
import com.signalcollect.interfaces.{ UndeliverableSignalHandler, UndeliverableSignalHandlerFactory }
import com.signalcollect.triplerush.{ ChildIdReply, ChildIdRequest }
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.vertices.PIndex
import com.signalcollect.triplerush.OperationIds
import com.signalcollect.triplerush.IndexStructure
import com.signalcollect.triplerush.ParticleDebug
import com.signalcollect.triplerush.IndexType

case class TripleRushUndeliverableSignalHandlerFactory(indexStructure: IndexStructure) extends UndeliverableSignalHandlerFactory[Long, Any] {

  def createInstance: UndeliverableSignalHandler[Long, Any] = new TripleRushUndeliverableSignalHandler(indexStructure)

  override def toString = "TripleRushUndeliverableSignalHandlerFactory"

}

case class TripleRushUndeliverableSignalHandler(indexStructure: IndexStructure) extends UndeliverableSignalHandler[Long, Any] {

  def vertexForSignalNotFound(signal: Any, inexistentTargetId: Long, senderId: Option[Long], graphEditor: GraphEditor[Long, Any]): Unit = {
    signal match {
      case queryParticle: Array[Int] =>
        if (!indexStructure.isSupported(inexistentTargetId)) {
          val errorMsg = s"Failed signal delivery of particle ${ParticleDebug(queryParticle).toString} " +
            s"to index with ID ${inexistentTargetId.toTriplePattern} of type ${IndexType(inexistentTargetId)}. This index type is not supported by the used index structure $indexStructure."
          graphEditor.log.error(new UnsupportedOperationException(errorMsg), errorMsg)
        }
        val queryVertexId = OperationIds.embedInLong(queryParticle.queryId)
        graphEditor.sendSignal(queryParticle.tickets, queryVertexId)
      case ChildIdRequest =>
        graphEditor.sendSignal(ChildIdReply(Array()), senderId.get, inexistentTargetId)
      case other: Any =>
        val errorMsg = if (inexistentTargetId.isOperationId) {
          s"Failed signal delivery of $other of type ${other.getClass} to " +
            s"the query vertex with query id ${OperationIds.extractFromLong(inexistentTargetId)} from sender id $senderId."
        } else {
          s"Failed signal delivery of $other of type ${other.getClass} to the " +
            s"index vertex ${inexistentTargetId.toTriplePattern} from sender id $senderId."
        }
        println(errorMsg)
        graphEditor.log.error(new UnsupportedOperationException(errorMsg), errorMsg)
    }
  }

}
