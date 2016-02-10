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
 * TODO: Test with counters > Int.MaxValue
 */
final class FifoQueue[@specialized I: ClassTag](minCapacity: Int) {
  assert(minCapacity > 1,
    "The minimum capacity needs to be larger than 1.")
  val capacity = nextPowerOfTwo(minCapacity)
  assert(capacity >= minCapacity,
    "There is no power of two in Int range that is larger than or equal to the min capacity.")
  private[this] val mask = capacity - 1

  private[this] val impl = new Array[I](capacity)
  private[this] var takeIndex = 0
  private[this] var _size = 0
  private[this] val emptyTakeAll = Array[I]()
  @inline private[this] def putIndex = (takeIndex + size) & mask
  @inline def size: Int = _size
  @inline def isEmpty: Boolean = _size == 0
  @inline def isFull: Boolean = _size == capacity
  @inline def freeCapacity: Int = capacity - _size

  val takeFailed: I = null.asInstanceOf[I]
  val batchTakeFailed: Array[I] = null.asInstanceOf[Array[I]]

  override def toString(): String = {
    s"FifoQueue(takeIndex=$takeIndex, size/capacity=$size/$capacity, array=${impl.mkString("[", ",", "]")}"
  }

  def put(item: I): Boolean = {
    if (isFull) {
      false
    } else {
      impl(putIndex) = item
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
          System.arraycopy(items, 0, impl, putIndex, itemCount)
        } else {
          System.arraycopy(items, 0, impl, putIndex, capacityOnRight)
          System.arraycopy(items, capacityOnRight, impl, 0, itemCount - capacityOnRight)
        }
        _size += itemCount
      }
      true
    } else {
      false
    }
  }

  def take(): I = {
    if (isEmpty) {
      takeFailed
    } else {
      val item = impl(takeIndex)
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
      val result = circularBufferCopy(takeIndex, _size)
      clear()
      result
    }
  }

  def batchTake(batchSize: Int): Array[I] = {
    if (batchSize > _size) {
      batchTakeFailed
    } else {
      val result = circularBufferCopy(takeIndex, batchSize)
      takeIndex = (takeIndex + batchSize) & mask
      _size -= batchSize
      result
    }
  }

  @inline private[this] def circularBufferCopy(fromIndex: Int, items: Int): Array[I] = {
    val result = new Array[I](items)
    val rightLength = capacity - fromIndex
    if (rightLength >= items) {
      System.arraycopy(impl, fromIndex, result, 0, items)
    } else {
      System.arraycopy(impl, fromIndex, result, 0, rightLength)
      System.arraycopy(impl, putIndex, result, rightLength, items - rightLength)
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

