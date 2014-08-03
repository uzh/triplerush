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
  id: Long) extends IndexVertex[Any](id) {

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    super.afterInitialization(graphEditor)
  }

  @inline final def childIdsContain(n: Int): Boolean = {
    state match {
      case i: Int =>
        if (i != n) false else true
      case a: Array[Int] =>
        new SearchableIntSet(a).contains(n)
    }
  }

  def handleChildIdRequest(requestor: Long, graphEditor: GraphEditor[Long, Any]) {
    state match {
      case i: Int =>
        graphEditor.sendSignal(ChildIdReply(Array(i)), requestor)
      case a: Array[Int] =>
        graphEditor.sendSignal(ChildIdReply(a.toArray), requestor)
    }
  }

  @inline def foreachChildDelta(f: Int => Unit) = {
    // state is not allowed to be null at this point.
    state match {
      case i: Int =>
        f(i)
      case a: Array[Int] =>
        var i = 0
        val l = a.length
        while (i < l) {
          f(a(i))
          i += 1
        }
    }
  }

  override def edgeCount = {
    if (state != null) {
      state match {
        case i: Int =>
          1
        case a: Array[Int] =>
          a.length
      }
    } else {
      0
    }
  }

  def cardinality = edgeCount

  def addChildDelta(delta: Int): Boolean = {
    if (state == null) {
      state = delta
      true
    } else {
      state match {
        case i: Int =>
          if (delta != i) {
            if (i > delta) {
              state = Array(delta, i)
            } else {
              state = Array(i, delta)
            }
            true
          } else {
            false
          }
        case a: Array[Int] =>
          val deltasBeforeInsert = a
          state = new SearchableIntSet(a).insert(delta)
          val wasInserted = deltasBeforeInsert != state // Reference comparison, if a new array was allocated, then an insert happened.
          wasInserted
      }
    }
  }

}

