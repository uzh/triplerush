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
import com.signalcollect.util.IntValueHashMap

case class QueryPlan(
  id: Set[TriplePattern],
  costSoFar: Double,
  estimatedTotalCost: Double,
  patternOrdering: List[TriplePattern] = Nil,
  fringe: Double = 0)

/**
 * Min heap that automatically replaces an existing plan with
 * the same id, if the newly inserted plan has a lower cost.
 *
 * Insert and remove are both O(log(n)).
 *
 */
final class QueryPlanMinHeap(initialSize: Int) {

  // TODO: Use IntValueHashMap instead?
  //  var indexMap = new IntValueHashMap[Set[TriplePattern]](
  //    initialSize = (initialSize * 1.3333).toInt,
  //    rehashFraction = 0.75f)

  var indexMap = Map.empty[Set[TriplePattern], Int]

  // Removes all elements from the heap.
  def toSortedArray: Array[QueryPlan] = {
    val size = numberOfElements
    val sorted = new Array[QueryPlan](size)
    var i = 0
    while (i < size) {
      sorted(i) = remove
      i += 1
    }
    sorted
  }

  var r = new Array[QueryPlan](initialSize)

  var numberOfElements: Int = 0
  val NOTHING: QueryPlan = null.asInstanceOf[QueryPlan]

  def isEmpty: Boolean = numberOfElements == 0

  def remove: QueryPlan = {
    // Retrieve min.
    numberOfElements -= 1
    val result = r(0)
    indexMap -= result.id

    // Restore heap.
    val lastElementIndex = numberOfElements
    val lastElement = r(lastElementIndex)
    // Move last element to the top of the heap and let it sift down.
    r(0) = lastElement
    r(lastElementIndex) = NOTHING
    indexMap(lastElement.id) = 0
    siftDown(0)

    // Return min.
    result
  }

  def insert(element: QueryPlan) {
    val existing = indexMap.get(element.id)
    if (existing.isDefined) {
      val indexOfExisting = existing.get
      if (r(indexOfExisting).estimatedTotalCost > element.estimatedTotalCost) {
        // Replace existing element.
        r(indexOfExisting) = element
        siftUp(indexOfExisting)
      }
    } else {
      if (numberOfElements == r.length) {
        // Double heap size.
        val newSize = r.length * 2
        val oldR = r
        r = new Array[QueryPlan](newSize)
        System.arraycopy(oldR, 0, r, 0, numberOfElements)
      }
      val index = numberOfElements
      numberOfElements += 1
      indexMap += element.id -> index
      r(index) = element
      siftUp(index)
    }
  }

  @tailrec def siftDown(index: Int) {
    if (index < numberOfElements) {
      val leftIndex = 2 * index + 1
      val rightIndex = leftIndex + 1
      val current = r(index)
      val currentValue = r(index).estimatedTotalCost
      if (rightIndex < numberOfElements) {
        val left = r(leftIndex)
        val right = r(rightIndex)
        val leftValue = left.estimatedTotalCost
        val rightValue = right.estimatedTotalCost
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
      } else if (leftIndex < numberOfElements) {
        // right == NOTHING
        val left = r(leftIndex)
        val leftValue = left.estimatedTotalCost
        if (leftValue < currentValue) {
          swap(index, leftIndex)
          // No sift down necessary anymore, the element to the right is already null.
        }
      }
    }
  }

  @inline def swap(i1: Int, i2: Int) {
    val r1 = r(i1)
    val r2 = r(i2)
    r(i1) = r2
    r(i2) = r1
    indexMap(r1.id) = i2
    indexMap(r2.id) = i1
  }

  @tailrec def siftUp(index: Int) {
    if (index > 0) {
      val currentValue = r(index).estimatedTotalCost
      val parentIndex = (index - 1) / 2
      val parentValue = r(parentIndex).estimatedTotalCost
      if (parentValue > currentValue) {
        swap(index, parentIndex)
        siftUp(parentIndex)
      }
    }
  }

}
