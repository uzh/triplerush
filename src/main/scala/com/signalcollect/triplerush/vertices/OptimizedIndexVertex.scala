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
import com.signalcollect.util.IntSet

abstract class OptimizedIndexVertex(
  id: TriplePattern) extends IndexVertex(id) {

  optimizedChildDeltas = Array()
  @transient var optimizedChildDeltas: Array[Byte] = _

  @transient var edgeCounter = 0

  def edgeCount = edgeCounter
  def cardinality = edgeCounter

  @inline def foreachChildDelta(f: Int => Unit) = new IntSet(optimizedChildDeltas).foreach(f)

  def addChildDelta(delta: Int): Boolean = {
    val alreadyContained = new IntSet(optimizedChildDeltas).contains(delta)
    if (alreadyContained) {
      false
    } else {
      optimizedChildDeltas = new IntSet(optimizedChildDeltas).insert(delta)
      edgeCounter += 1
      true
    }
  }

}
