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

import com.signalcollect.GraphEditor
import QueryParticle.arrayToParticle
import com.signalcollect.triplerush.vertices.PIndex
import com.signalcollect.triplerush.EfficientIndexPattern._

case object UndeliverableRerouter {
  def handle(signal: Any, targetId: Long, sourceId: Option[Long], graphEditor: GraphEditor[Long, Any]) {
    signal match {
      case queryParticle: Array[Int] =>
        val queryVertexId = QueryIds.embedQueryIdInLong(queryParticle.queryId)
        graphEditor.sendSignal(queryParticle.tickets, queryVertexId, None)
      case CardinalityRequest(forPattern: TriplePattern, requestor: Long) =>
        graphEditor.sendSignal(CardinalityReply(forPattern, 0), requestor, None)
      case ChildIdRequest =>
        graphEditor.sendSignal(ChildIdReply(Array()), sourceId.get, Some(targetId))
      case s: SubjectCountSignal =>
        // This count could potentially arrive before the vertex is created.
        val predicateIndex = new PIndex(targetId.asInstanceOf[Long])
        graphEditor.addVertex(predicateIndex)
        graphEditor.sendSignal(signal, targetId, sourceId)
      case other =>
        targetId match {
          case indexOrQueryId: Long =>
            if (indexOrQueryId.isQueryId) {
              println(s"Failed signal delivery of $other of type ${other.getClass} to the query vertex with query id ${QueryIds.extractQueryIdFromLong(targetId)} and sender id $sourceId.")
            } else {
              println(s"Failed signal delivery of $other of type ${other.getClass} to the index vertex ${targetId.toTriplePattern} and sender id $sourceId.")
            }
          case _ =>
            println(s"Failed signal delivery of $other of type ${other.getClass} to the vertex with id $targetId and sender id $sourceId.")
        }
    }
  }
}
