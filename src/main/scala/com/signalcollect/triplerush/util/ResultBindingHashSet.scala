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
 */

package com.signalcollect.triplerush.util

import scala.Array.canBuildFrom
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3._
import annotation.switch

class ResultBindingWrapper(val bindings: Array[Int]) extends AnyVal {

  def equalLike(other: Array[Int]): Boolean = {
    val length = bindings.length
    if (length == other.length) {
      var i = 0
      while (i < length) {
        if (bindings(i) != other(i)) {
          return false
        }
        i += 1
      }
    } else {
      false
    }
    true
  }

  /**
   * Only hash up to the first three bindings.
   */
  def hashCodeLike: Int = {
    val length = bindings.length
    (length: @switch) match {
      case 0 => 0
      case 1 => bindings(0)
      case 2 => finalizeHash(mixLast(bindings(0), bindings(1)), 3)
      case other => finalizeHash(mixLast(mix(bindings(0), bindings(1)), bindings(2)), 3)
    }
  }
}

class ResultBindingsHashSet(
  initialSize: Int = 32768,
  rehashFraction: Float = 0.75f) {
  assert(initialSize > 0)
  final var maxSize = nextPowerOfTwo(initialSize)
  assert(1.0f >= rehashFraction && rehashFraction > 0.1f, "Unreasonable rehash fraction.")
  assert(maxSize > 0 && maxSize >= initialSize, "Initial size is too large.")
  private[this] final var maxElements: Int = (rehashFraction * maxSize).floor.toInt
  private[this] final var keys = new Array[Array[Int]](maxSize) // 0 means empty
  private[this] final var mask = maxSize - 1
  private[this] final var nextPositionToProcess = 0

  final def size: Int = numberOfElements
  final def isEmpty: Boolean = numberOfElements == 0
  private[this] final var numberOfElements = 0

  final def clear {
    keys = new Array[Array[Int]](maxSize)
    numberOfElements = 0
    nextPositionToProcess = 0
  }

  def toScalaSet: Set[Array[Int]] = {
    keys.filter(_ != null).toSet
  }

  private[this] final def tryDouble {
    // 1073741824 is the largest size and cannot be doubled anymore.
    if (maxSize != 1073741824) {
      val oldKeys = keys
      val oldNumberOfElements = numberOfElements
      maxSize *= 2
      maxElements = (rehashFraction * maxSize).floor.toInt
      keys = new Array[Array[Int]](maxSize)
      mask = maxSize - 1
      numberOfElements = 0
      var i = 0
      var elementsMoved = 0
      while (elementsMoved < oldNumberOfElements) {
        val oldKey = oldKeys(i)
        if (oldKey != null) {
          add(oldKey)
          elementsMoved += 1
        }
        i += 1
      }
    }
  }

  final def foreach(f: Array[Int] => Unit) {
    var i = 0
    var elementsProcessed = 0
    while (elementsProcessed < numberOfElements) {
      val key = keys(i)
      if (key != null) {
        f(key)
        elementsProcessed += 1
      }
      i += 1
    }
  }

  @inline final def contains(key: Array[Int]): Boolean = {
    var position = keyToPosition(key)
    var keyAtPosition = keys(position)
    val searchKey = new ResultBindingWrapper(key)
    while (keyAtPosition != null && !searchKey.equalLike(keyAtPosition)) {
      position = (position + 1) & mask
      keyAtPosition = keys(position)
    }
    if (keyAtPosition != null) {
      true
    } else {
      false
    }
  }

  /**
   * Returns true iff this entry already existed.
   */
  def add(key: Array[Int]): Boolean = {
    assert(key != null, "Key cannot be null")
    val addKey = new ResultBindingWrapper(key)
    var position = keyToPosition(key)
    var keyAtPosition = keys(position)
    while (keyAtPosition != null && !addKey.equalLike(keyAtPosition)) {
      position = (position + 1) & mask
      keyAtPosition = keys(position)
    }
    val alreadyContained = keyAtPosition != null && addKey.equalLike(keyAtPosition)
    if (!alreadyContained) {
      keys(position) = key
      numberOfElements += 1
      if (numberOfElements >= maxElements) {
        tryDouble
        if (numberOfElements >= maxSize) {
          throw new OutOfMemoryError("The hash map is full and cannot be expanded any further.")
        }
      }
    }
    alreadyContained
  }

  private[this] final def keyToPosition(key: Array[Int]) = {
    new ResultBindingWrapper(key).hashCodeLike & mask
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
