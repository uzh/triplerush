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
import com.signalcollect.util.SearchableIntSet

/**
 * Stores the child delta array in the state.
 */
abstract class SearchableIndexVertex[SignalType, State](
  id: Long) extends IndexVertex[Array[Int]](id) {

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    super.afterInitialization(graphEditor)
    state = Array[Int]()
  }

  def handleChildIdRequest(requestor: Long, graphEditor: GraphEditor[Long, Any]) {
    graphEditor.sendSignal(ChildIdReply(state.toBuffer.toArray), requestor)
  }

  @inline def foreachChildDelta(f: Int => Unit) = {
    var i = 0
    val l = state.length
    while (i < l) {
      f(state(i))
      i += 1
    }
  }

  override def edgeCount = {
    if (state != null) state.length else 0
  }

  def cardinality = state.length

  def forechildDeltas: Traversable[Int] = state

  def addChildDelta(delta: Int): Boolean = {
    val deltasBeforeInsert = state
    state = new SearchableIntSet(state).insert(delta)
    val wasInserted = deltasBeforeInsert != state // Reference comparison, if a new array was allocated, then an insert happened.
    wasInserted
  }

}

