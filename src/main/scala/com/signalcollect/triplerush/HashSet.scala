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
 */

package com.signalcollect.triplerush

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import com.signalcollect.Vertex
import com.signalcollect.interfaces.VertexStore

/**
 * Can only be used for positive integers. 
 */
class HashSet(
  initialSize: Int = 2,
  rehashFraction: Float = 0.75f) extends Traversable[Int] {
  assert(initialSize > 0)
  final var maxSize = nextPowerOfTwo(initialSize)
  assert(1.0f >= rehashFraction && rehashFraction > 0.1f, "Unreasonable rehash fraction.")
  assert(maxSize > 0 && maxSize >= initialSize, "Initial size is too large.")
  private[this] final var maxElements: Int = (rehashFraction * maxSize).floor.toInt
  private[this] final var items = new Array[Int](maxSize) // 0 means no item at position
  private[this] final var mask = maxSize - 1
  private[this] final var nextPositionToProcess = 0

  final override def size = numberOfElements
  final override def isEmpty = numberOfElements == 0
  private[this] final var numberOfElements = 0

  final def foreach[U](f: Int => U) {
    var i = 0
    var elementsProcessed = 0
    while (elementsProcessed < numberOfElements) {
      val item = items(i)
      if (item != 0) {
        f(item)
        elementsProcessed += 1
      }
      i += 1
    }
  }

  private[this] final def tryDouble {
    // 1073741824 is the largest size and cannot be doubled anymore.
    if (maxSize != 1073741824) {
      val oldSize = maxSize
      val oldItems = items
      val oldNumberOfElements = numberOfElements
      maxSize *= 2
      maxElements = (rehashFraction * maxSize).floor.toInt
      items = new Array[Int](maxSize)
      mask = maxSize - 1
      numberOfElements = 0
      var i = 0
      var elementsMoved = 0
      while (elementsMoved < oldNumberOfElements) {
        val item = oldItems(i)
        if (item != 0) {
          add(item)
          elementsMoved += 1
        }
        i += 1
      }
    }
  }

  final def remove(item: Int) {
    remove(item, true)
  }

  private final def remove(item: Int, optimize: Boolean) {
    var position = itemToPosition(item)
    var itemAtPosition = items(position)
    while (itemAtPosition != 0 && item != itemAtPosition) {
      position = (position + 1) & mask
      itemAtPosition = items(position)
    }
    // We can only remove the entry if it was found.
    if (itemAtPosition != 0) {
      items(position) = 0
      numberOfElements -= 1
      if (optimize) {
        optimizeFromPosition((position + 1) & mask)
      }
    }
  }

  // Try to reinsert all items that are not optimally placed until an empty position is found.
  // See http://stackoverflow.com/questions/279539/best-way-to-remove-an-entry-from-a-hash-table
  private[this] final def optimizeFromPosition(startingPosition: Int) {
    var currentPosition = startingPosition
    var itemAtPosition = items(currentPosition)
    while (isCurrentPositionOccupied) {
      val perfectPositionForItem = itemToPosition(itemAtPosition)
      if (perfectPositionForItem != currentPosition) {
        // We try to optimize the placement of the entry by removing and then reinserting it.
        val item = items(currentPosition)
        removeCurrentEntry
        add(itemAtPosition)
      }
      advance
    }
    def advance {
      currentPosition = ((currentPosition + 1) & mask)
      itemAtPosition = items(currentPosition)
    }
    def isCurrentPositionOccupied = {
      itemAtPosition != 0
    }
    def removeCurrentEntry {
      items(currentPosition) = 0
      numberOfElements -= 1
    }
  }

  final def contains(item: Int): Boolean = {
    var position = itemToPosition(item)
    var itemAtPosition = items(position)
    while (itemAtPosition != 0 && item != itemAtPosition) {
      position = (position + 1) & mask
      itemAtPosition = items(position)
    }
    itemAtPosition != 0
  }

  final def add(item: Int): Boolean = {
    var position = itemToPosition(item)
    var itemAtPosition = items(position)
    while (itemAtPosition != 0 && item != itemAtPosition) { {
        position = (position + 1) & mask
        itemAtPosition = items(position)
      }
    }
    var doAdd = itemAtPosition == 0
    // Only add if no such item has been added yet. 
    if (doAdd) {
      items(position) = item
      numberOfElements += 1
      if (numberOfElements >= maxElements) {
        tryDouble
        if (numberOfElements >= maxSize) {
          throw new OutOfMemoryError("The hash map is full and cannot be expanded any further.")
        }
      }
    }
    doAdd
  }

  private[this] final def itemToPosition(item: Int) = {
    item & mask
  }

  private[this] final def nextPowerOfTwo(x: Int): Int = {
    var r = x - 1
    r |= r >> 1
    r |= r >> 2
    r |= r >> 4
    r |= r >> 8
    r |= r >> 16
    r + 1
  }

}