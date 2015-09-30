/*
 *  @author Philip Stutz
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

package com.signalcollect.triplerush.vertices

import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.util.SplayIntSet
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.OperationIds

final class RootIndex extends OptimizedIndexVertex(EfficientIndexPattern(0, 0, 0))
    with Forwarding[Any] {

  override def processQuery(query: Array[Int], graphEditor: GraphEditor[Long, Any]): Unit = {
    // Root index is the only one that might have no outgoing edges.
    if (edgeCount == 0) {
      // No outgoing edges: Fail query immediately.
      val queryVertexId = OperationIds.embedInLong(query.queryId)
      graphEditor.sendSignal(query.tickets, queryVertexId)
    } else {
      super.processQuery(query, graphEditor)
    }
  }

  override def addChildDelta(delta: Int): Boolean = {
    println(s"adding delta $delta to root vertex")
    super.addChildDelta(delta)
  }
  
  def nextRoutingAddress(childDelta: Int): Long = EfficientIndexPattern(0, childDelta, 0)

}
