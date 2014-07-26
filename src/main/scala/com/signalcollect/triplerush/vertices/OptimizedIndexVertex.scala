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
import com.signalcollect.util.Ints
import com.signalcollect.util.FastInsertIntSet
import com.signalcollect.util.SplayNode

/**
 * Stores the SplayIntSet with the optimized child deltas in the state.
 */
abstract class OptimizedIndexVertex(
  id: Long) extends IndexVertex[Any](id) {

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    super.afterInitialization(graphEditor)
    //state = new MemoryEfficientSplayIntSet
    //      def overheadFraction = 0.01f
    //  def maxNodeIntSetSize = 1000
    //      val repr = Ints.createEmptyFastInsertIntSet
    //      new FastInsertIntSet(repr).insert(i, overheadFraction)
    state = Ints.createEmptyFastInsertIntSet
  }

  def handleChildIdRequest(requestor: Long, graphEditor: GraphEditor[Long, Any]) {
    val childIds = state match {
      case a: Array[Byte] =>
        new FastInsertIntSet(a).toBuffer
      case s: SplayIntSet =>
        s.toBuffer
    }
    graphEditor.sendSignal(ChildIdReply(childIds.toArray), requestor)
  }

  override def edgeCount = {
    if (state != null) numberOfStoredChildDeltas else 0
  }

  def cardinality = numberOfStoredChildDeltas

  @inline final def numberOfStoredChildDeltas = {
    state match {
      case a: Array[Byte] =>
        new FastInsertIntSet(a).size
      case s: SplayIntSet =>
        s.size
    }
  }

  @inline def foreachChildDelta(f: Int => Unit) = {
    state match {
      case a: Array[Byte] =>
        new FastInsertIntSet(a).foreach(f)
      case s: SplayIntSet =>
        s.foreach(f)
    }
  }

  def addChildDelta(delta: Int): Boolean = {
    state match {
      case a: Array[Byte] =>
        val sizeBefore = new FastInsertIntSet(a).size
        val intSetAfter = new FastInsertIntSet(a).insert(delta, 0.01f)
        val sizeAfter = new FastInsertIntSet(intSetAfter).size
        
        if (sizeAfter >= 1000) {
          val splayIntSet = new MemoryEfficientSplayIntSet
          val root = new SplayNode(intSetAfter)
          splayIntSet.initializeWithRoot(root)
          state = splayIntSet
        } else {
          state = intSetAfter
        }
        sizeAfter > sizeBefore
      case s: SplayIntSet =>
        val wasInserted = s.insert(delta)
        wasInserted
    }
  }

}
