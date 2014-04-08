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

import com.signalcollect.util.Ints._
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.util.SearchableIntSet
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.ChildIdRequest
import com.signalcollect.triplerush.ChildIdReply

abstract class SearchableIndexVertex[SignalType, State](
  id: TriplePattern) extends IndexVertex(id) {

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    super.afterInitialization(graphEditor)
    childDeltaArray = Array[Int]()
  }

  def handleChildIdRequest(requestor: Any, graphEditor: GraphEditor[Any, Any]) {
    graphEditor.sendSignal(ChildIdReply(childDeltaArray.toBuffer.toArray), requestor, None)
  }

  @transient var childDeltaArray: Array[Int] = _

  @inline def foreachChildDelta(f: Int => Unit) = {
    var i = 0
    val l = childDeltaArray.length
    while (i < l) {
      f(childDeltaArray(i))
      i += 1
    }
  }

  override def edgeCount = {
    if (childDeltaArray != null) childDeltaArray.length else 0
  }

  def cardinality = childDeltaArray.length

  def forechildDeltas: Traversable[Int] = childDeltaArray

  def addChildDelta(delta: Int): Boolean = {
    val deltasBeforeInsert = childDeltaArray
    childDeltaArray = new SearchableIntSet(childDeltaArray).insert(delta)
    val wasInserted = deltasBeforeInsert != childDeltaArray
    wasInserted
  }

}
