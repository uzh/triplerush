/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

package com.signalcollect.triplerush.optimizers

import scala.annotation.tailrec
import collection.mutable.Map
import com.signalcollect.triplerush.TriplePattern

case class QueryPlan(
  id: Set[TriplePattern],
  cost: Double,
  patternOrdering: List[TriplePattern] = Nil,
  fringe: Double = 0)

/**
 * Min heap that automatically replaces an existing plan with
 * the same id, if the newly inserted plan has a lower cost.
 *
 * Insert and remove are both O(log(n)).
 */
final class QueryPlanMinHeap(maxSize: Int) {

  var indexMap = Map.empty[Set[TriplePattern], Int]

  def validateIndexMap {
    for ((key, value) <- indexMap) {
      if (r(value).id != key) {
        println(r.toVector + "\n" + indexMap)
        throw new Exception("Mooh")
      }
    }
  }

  // Removes all elements from the heap.
  def toSortedArray: Array[QueryPlan] = {
    try {
      val size = numberOfElements
      val sorted = new Array[QueryPlan](size)
      var i = 0
      while (i < size) {
        sorted(i) = remove
        i += 1
      }
      sorted
    } catch {
      case t: Throwable =>
        t.printStackTrace
        throw t
    }
  }

  val r = new Array[QueryPlan](maxSize)

  var numberOfElements: Int = 0
  val NOTHING: QueryPlan = null.asInstanceOf[QueryPlan]

  def isEmpty: Boolean = numberOfElements == 0

  def remove: QueryPlan = {
    assert(r(0) != NOTHING)
    numberOfElements -= 1
    val result = r(0)
    indexMap -= result.id
    r(0) = NOTHING
    siftDown(0)
    result
  }

  def insert(element: QueryPlan) {
    val existing = indexMap.get(element.id)
    if (existing.isDefined) {
      val indexOfExisting = existing.get
      if (r(indexOfExisting).cost > element.cost) {
        // Replace existing element.
        r(indexOfExisting) = element
        siftUp(indexOfExisting)
      }
    } else {
      val index = numberOfElements
      assert(r(index) == NOTHING)
      indexMap += element.id -> index
      r(index) = element
      numberOfElements += 1
      siftUp(index)
    }
  }

  @tailrec def siftDown(index: Int) {
    val leftIndex = 2 * index + 1
    val rightIndex = leftIndex + 1
    val current = r(index)
    val left = if (leftIndex < maxSize) r(leftIndex) else NOTHING
    val right = if (rightIndex < maxSize) r(rightIndex) else NOTHING
    if (current == NOTHING) {
      if (left != NOTHING && right != NOTHING) {
        val leftValue = left.cost
        val rightValue = right.cost
        if (leftValue < rightValue) {
          swap(index, leftIndex)
          siftDown(leftIndex)
        } else {
          swap(index, rightIndex)
          siftDown(rightIndex)
        }
      } else if (left != NOTHING) {
        val leftValue = left.cost
        swap(index, leftIndex)
        siftDown(leftIndex)
      } else if (right != NOTHING) {
        swap(index, rightIndex)
        siftDown(rightIndex)
      }
    } else {
      val currentValue = r(index).cost
      if (left != NOTHING && right != NOTHING) {
        val leftValue = left.cost
        val rightValue = right.cost
        if (leftValue < rightValue) {
          if (leftValue < currentValue) {
            swap(index, leftIndex)
            siftDown(leftIndex)
          }
        } else {
          if (rightValue < currentValue) {
            swap(index, rightIndex)
            siftDown(rightIndex)
          }
        }
      } else if (left != NOTHING) {
        val leftValue = left.cost
        if (leftValue < currentValue) {
          swap(index, leftIndex)
          siftDown(leftIndex)
        }
      } else if (right != NOTHING) {
        val rightValue = right.cost
        if (rightValue < currentValue) {
          swap(index, rightIndex)
          siftDown(rightIndex)
        }
      }
    }
  }

  @inline def swap(i1: Int, i2: Int) {
    val r1 = r(i1)
    val r2 = r(i2)
    r(i1) = r2
    if (r1 != NOTHING) {
      indexMap(r1.id) = i2
    }
    r(i2) = r1
    if (r2 != NOTHING) {
      indexMap(r2.id) = i1
    }
  }

  @tailrec def siftUp(index: Int) {
    if (index > 0) {
      val currentValue = r(index).cost
      val parentIndex = (index - 1) / 2
      val parentValue = r(parentIndex).cost
      if (parentValue > currentValue) {
        swap(index, parentIndex)
        siftUp(parentIndex)
      }
    }
  }

}