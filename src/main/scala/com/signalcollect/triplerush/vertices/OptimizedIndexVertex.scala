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

package com.signalcollect.triplerush.vertices

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.ChildIdReply
import com.signalcollect.triplerush.util.MemoryEfficientSplayIntSet
import com.signalcollect.util.SplayIntSet

/**
 * Stores the SplayIntSet with the optimized child deltas in the state.
 */
abstract class OptimizedIndexVertex(
  id: Long) extends IndexVertex[SplayIntSet](id) {

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    super.afterInitialization(graphEditor)
    state = new MemoryEfficientSplayIntSet
  }

  def handleChildIdRequest(requestor: Long, graphEditor: GraphEditor[Long, Any]) {
    graphEditor.sendSignal(ChildIdReply(state.toBuffer.toArray), requestor)
  }

  override def edgeCount = {
    if (state != null) state.size else 0
  }
  
  def cardinality = state.size

  @inline def foreachChildDelta(f: Int => Unit) = state.foreach(f)

  def addChildDelta(delta: Int): Boolean = {
    val deltasBeforeInsert = state
    val wasInserted = state.insert(delta)
    wasInserted
  }

}
