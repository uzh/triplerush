/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 iHealth Technologies
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

package com.signalcollect.triplerush.dictionary

import com.signalcollect.Vertex
import com.signalcollect.interfaces.VertexStore
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

final class ByteArrayHashMap(
    initialSize: Int = 32768,
    rehashFraction: Float = 0.75f) {
  assert(initialSize > 0)
  var maxSize = nextPowerOfTwo(initialSize)
  assert(1.0f >= rehashFraction && rehashFraction > 0.1f, "Unreasonable rehash fraction.")
  assert(maxSize > 0 && maxSize >= initialSize, "Initial size is too large.")
  private[this] final var maxElements: Int = (rehashFraction * maxSize).floor.toInt
  private[this] final var values = new Array[Int](maxSize)
  private[this] final var keys = new Array[Array[Byte]](maxSize) // 0 means empty
  private[this] final var mask = maxSize - 1
  private[this] final var nextPositionToProcess = 0

  final def size: Int = numberOfElements
  final def isEmpty: Boolean = numberOfElements == 0
  private[this] final var numberOfElements = 0

  def clear {
    keys = new Array[Array[Byte]](maxSize)
    numberOfElements = 0
    nextPositionToProcess = 0
  }

  private[this] final def tryDouble: Unit = {
    // 1073741824 is the largest size and cannot be doubled anymore.
    if (maxSize != 1073741824) {
      val oldValues = values
      val oldKeys = keys
      val oldNumberOfElements = numberOfElements
      maxSize *= 2
      maxElements = (rehashFraction * maxSize).floor.toInt
      values = new Array[Int](maxSize)
      keys = new Array[Array[Byte]](maxSize)
      mask = maxSize - 1
      numberOfElements = 0
      var i = 0
      var elementsMoved = 0
      while (elementsMoved < oldNumberOfElements) {
        val oldKey = oldKeys(i)
        if (oldKey != null) {
          put(oldKey, oldValues(i))
          elementsMoved += 1
        }
        i += 1
      }
    }
  }

  final def foreach(f: (Array[Byte], Int) => Unit) {
    var i = 0
    var elementsProcessed = 0
    while (elementsProcessed < numberOfElements) {
      val key = keys(i)
      if (key != null) {
        val value = values(i)
        f(key, value)
        elementsProcessed += 1
      }
      i += 1
    }
  }

  final def remove(key: Array[Byte]) {
    remove(key, true)
  }

  private final def remove(key: Array[Byte], optimize: Boolean) {
    var position = keyToPosition(key)
    var keyAtPosition = keys(position)
    while (keyAtPosition != null && !equalArray(key, keyAtPosition)) {
      position = (position + 1) & mask
      keyAtPosition = keys(position)
    }
    // We can only remove the entry if it was found.
    if (keyAtPosition != null) {
      keys(position) = null.asInstanceOf[Array[Byte]]
      numberOfElements -= 1
      if (optimize) {
        optimizeFromPosition((position + 1) & mask)
      }
    }
  }

  // Try to reinsert all elements that are not optimally placed until an empty position is found.
  // See http://stackoverflow.com/questions/279539/best-way-to-remove-an-entry-from-a-hash-table
  private[this] final def optimizeFromPosition(startingPosition: Int) {
    var currentPosition = startingPosition
    var keyAtPosition = keys(currentPosition)
    while (isCurrentPositionOccupied) {
      val perfectPositionForEntry = keyToPosition(keyAtPosition)
      if (perfectPositionForEntry != currentPosition) {
        // We try to optimize the placement of the entry by removing and then reinserting it.
        val value = values(currentPosition)
        removeCurrentEntry
        put(keyAtPosition, value)
      }
      advance
    }
    @inline def advance {
      currentPosition = ((currentPosition + 1) & mask)
      keyAtPosition = keys(currentPosition)
    }
    @inline def isCurrentPositionOccupied = {
      keyAtPosition != null
    }
    @inline def removeCurrentEntry {
      keys(currentPosition) = null.asInstanceOf[Array[Byte]]
      numberOfElements -= 1
    }
  }

  final def apply(key: Array[Byte]) = get(key)

  @inline final def get(key: Array[Byte]): Int = {
    var position = keyToPosition(key)
    var keyAtPosition = keys(position)
    while (keyAtPosition != null && !equalArray(key, keyAtPosition)) {
      position = (position + 1) & mask
      keyAtPosition = keys(position)
    }
    if (keyAtPosition != null) {
      values(position)
    } else {
      0
    }
  }

  @inline final def increment(key: Array[Byte]) = {
    var position = keyToPosition(key)
    var keyAtPosition = keys(position)
    while (keyAtPosition != null && !equalArray(key, keyAtPosition)) {
      position = (position + 1) & mask
      keyAtPosition = keys(position)
    }
    if (keyAtPosition != null) {
      // Increment existing
      values(position) += 1
    } else {
      put(key, 1)
    }
  }

  def update(key: Array[Byte], value: Int) {
    put(key, value)
  }

  /**
   * Key 0 is not allowed!
   * Returns if an existing entry was overridden.
   */
  def put(key: Array[Byte], value: Int): Boolean = {
    assert(key != null, "Key cannot be null")
    var position = keyToPosition(key)
    var keyAtPosition = keys(position)
    while (keyAtPosition != null && !equalArray(key, keyAtPosition)) {
      position = (position + 1) & mask
      keyAtPosition = keys(position)
    }
    val overridden = keyAtPosition == key
    if (!overridden) {
      keys(position) = key
      values(position) = value
      numberOfElements += 1
      if (numberOfElements >= maxElements) {
        tryDouble
        if (numberOfElements >= maxSize) {
          throw new OutOfMemoryError("The hash map is full and cannot be expanded any further.")
        }
      }
    } else {
      values(position) = value
    }
    overridden
  }

  /**
   * Can only be called if both arrays are not null.
   */
  @inline def equalArray(a: Array[Byte], b: Array[Byte]): Boolean = {
    val aL = a.length
    val bL = b.length
    if (aL != bL) {
      return false
    } else {
      // Compare from back, as many IRIs share prefixes
      var i = aL - 1
      while (i >= 0) {
        if (a(i) != b(i)) {
          return false
        }
        i -= 1
      }
    }
    true
  }

  private[this] final def keyToPosition(key: Array[Byte]) = {
    // Based on the String hash code implementation.
    var hash = 2147483647 // large prime.
    var i = 0
    val l = key.length
    while (i < l) {
      hash = 31 * hash + key(i)
      i += 1
    }
    hash & mask
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
