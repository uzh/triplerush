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

abstract class SearchableIndexVertex[SignalType, State](
  id: TriplePattern) extends IndexVertex(id) {

  childDeltaArray = Array()
  @transient var childDeltaArray: Array[Int] = _

  @inline def foreachChildDelta(f: Int => Unit) = {
    var i = 0
    val l = childDeltaArray.length
    while (i < l) {
      f(childDeltaArray(i))
      i += 1
    }
  }

  def edgeCount = childDeltaArray.length

  def cardinality = childDeltaArray.length

  def forechildDeltas: Traversable[Int] = childDeltaArray

  def addChildDelta(delta: Int): Boolean = {
    val deltasBeforeInsert = childDeltaArray
    childDeltaArray = new SearchableIntSet(childDeltaArray).insert(delta)
    val wasInserted = deltasBeforeInsert != childDeltaArray
    wasInserted
  }

}
