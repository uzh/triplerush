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

case object UndeliverableRerouter {
  def handle(signal: Any, targetId: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) {
    signal match {
      case queryParticle: Array[Int] =>
        graphEditor.sendSignal(queryParticle.tickets, queryParticle.queryId, None)
      case CardinalityRequest(forPattern: TriplePattern, requestor: AnyRef) =>
        graphEditor.sendSignal(CardinalityReply(forPattern, 0), requestor, None)
      case ChildIdRequest =>
        graphEditor.sendSignal(ChildIdReply(Set()), sourceId.get, Some(targetId))    
      case other =>
        println(s"Failed signal delivery of $other of type ${other.getClass} to the vertex with id $targetId and sender id $sourceId.")
    }
  }
}
