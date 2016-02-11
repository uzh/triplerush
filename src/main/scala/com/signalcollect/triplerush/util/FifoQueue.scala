/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush.util

import scala.reflect.ClassTag

/**
 * FIFO queue implemented with a circular buffer.
 *
 * There needs to exist an Int that is >= `minCapacity` and that is a power of two.
 */
final class FifoQueue[@specialized I: ClassTag](minCapacity: Int) {
  assert(minCapacity > 1,
    "The minimum capacity needs to be larger than 1.")
  val capacity = nextPowerOfTwo(minCapacity)
  assert(capacity >= minCapacity,
    "There is no power of two in Int range that is larger than or equal to the min capacity.")
  private[this] val mask = capacity - 1

  private[this] val circularBuffer = new Array[I](capacity)
  private[this] var takeIndex = 0
  private[this] var _size = 0
  private[this] val emptyTakeAll = Array[I]()
  @inline private[this] def putIndex = (takeIndex + size) & mask
  @inline def size: Int = _size
  @inline def isEmpty: Boolean = _size == 0
  @inline def isFull: Boolean = _size == capacity
  @inline def freeCapacity: Int = capacity - _size

  val itemAccessFailed: I = null.asInstanceOf[I]
  val batchAccessFailed: Array[I] = null.asInstanceOf[Array[I]]

  override def toString(): String = {
    s"""FifoQueue(
  takeIndex = $takeIndex,
  size/capacity = $size/$capacity,
  peek = $peek
  buffer (first 16 items, up to 8 characters) = ${
      circularBuffer.take(16).map(_.toString().take(8)).mkString("\n  [\n    ", ",\n    ", "\n  ]")
    }
)
"""
  }

  def put(item: I): Boolean = {
    if (isFull) {
      false
    } else {
      circularBuffer(putIndex) = item
      _size += 1
      true
    }
  }

  def batchPut(items: Array[I]): Boolean = {
    val itemCount = items.length
    if (freeCapacity >= itemCount) {
      val capacityOnRight = capacity - putIndex
      if (itemCount > 0) {
        if (capacityOnRight >= itemCount) {
          System.arraycopy(
            items, 0,
            circularBuffer, putIndex,
            itemCount)
        } else {
          val requiredCapacityFromLeft = itemCount - capacityOnRight
          System.arraycopy(
            items, 0,
            circularBuffer, putIndex,
            capacityOnRight)
          System.arraycopy(
            items, capacityOnRight,
            circularBuffer, 0,
            requiredCapacityFromLeft)
        }
        _size += itemCount
      }
      true
    } else {
      false
    }
  }

  def peek(): I = {
    if (size == 0) {
      itemAccessFailed
    } else {
      circularBuffer(takeIndex)
    }
  }

  def take(): I = {
    if (isEmpty) {
      itemAccessFailed
    } else {
      val item = circularBuffer(takeIndex)
      takeIndex = (takeIndex + 1) & mask
      _size -= 1
      item
    }
  }

  @inline def clear(): Unit = {
    takeIndex = 0
    _size = 0
  }

  def takeAll(): Array[I] = {
    if (_size == 0) {
      emptyTakeAll
    } else {
      val copy = copyFromCircularBuffer(
        circularBuffer, startIndex = takeIndex, length = _size)
      clear()
      copy
    }
  }

  def batchTake(batchSize: Int): Array[I] = {
    if (batchSize > _size) {
      batchAccessFailed
    } else {
      val copy = copyFromCircularBuffer(
        circularBuffer, startIndex = takeIndex, length = batchSize)
      if (_size == batchSize) {
        clear()
      } else {
        _size -= batchSize
        takeIndex = (takeIndex + batchSize) & mask
      }
      copy
    }
  }

  def batchProcessAtMost(atMost: Int, p: I => Unit): Unit = {
    val toProcess = math.min(atMost, _size)
    var processed = 0
    while (processed < toProcess) {
      p(circularBuffer(takeIndex))
      takeIndex = (takeIndex + 1) & mask
      processed += 1
    }
    _size -= processed
  }

  @inline private[this] def copyFromCircularBuffer(
    buffer: Array[I], startIndex: Int, length: Int): Array[I] = {
    val result = new Array[I](length)
    val rightFragmentLength = capacity - startIndex
    if (rightFragmentLength >= length) {
      // Only need one copy, right fragment is long enough.
      System.arraycopy(
        buffer, startIndex,
        result, 0,
        length)
    } else {
      // Need 2 copies for the 2 fragments.
      val remainingLengthFromLeft = length - rightFragmentLength
      System.arraycopy(
        buffer, startIndex,
        result, 0,
        rightFragmentLength)
      System.arraycopy(
        buffer, putIndex,
        result, rightFragmentLength,
        remainingLengthFromLeft)
    }
    result
  }

  private[this] def nextPowerOfTwo(x: Int): Int = {
    var r = x - 1
    r |= r >> 1
    r |= r >> 2
    r |= r >> 4
    r |= r >> 8
    r |= r >> 16
    r + 1
  }

}

