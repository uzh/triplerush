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
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.vertices.PIndex
import com.signalcollect.triplerush.OperationIds

case object TripleRushUndeliverableSignalHandlerFactory extends UndeliverableSignalHandlerFactory[Long, Any] {
  def createInstance: UndeliverableSignalHandler[Long, Any] = TripleRushUndeliverableSignalHandler

  override def toString = "TripleRushUndeliverableSignalHandlerFactory"
}

case object TripleRushUndeliverableSignalHandler extends UndeliverableSignalHandler[Long, Any] {
  def vertexForSignalNotFound(signal: Any, inexistentTargetId: Long, senderId: Option[Long], graphEditor: GraphEditor[Long, Any]): Unit = {
    signal match {
      case queryParticle: Array[Int] =>
        val queryVertexId = OperationIds.embedInLong(queryParticle.queryId)
        graphEditor.sendSignal(queryParticle.tickets, queryVertexId)
      case ChildIdRequest =>
        graphEditor.sendSignal(ChildIdReply(Array()), senderId.get, inexistentTargetId)
      case other: Any =>
        if (inexistentTargetId.isOperationId) {
          println(s"Failed signal delivery of $other of type ${other.getClass} to " +
            s"the query vertex with query id ${OperationIds.extractFromLong(inexistentTargetId)} and sender id $senderId.")
        } else {
          println(s"Failed signal delivery of $other of type ${other.getClass} to the " +
            s"index vertex ${inexistentTargetId.toTriplePattern} and sender id $senderId.")
        }
    }
  }
}
