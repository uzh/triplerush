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

package com.signalcollect.triplerush.util

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.TimeUnit

/**
 * Only supports a single result consumer thread and a single producer.
 */
final class ResultIterator extends Iterator[Array[Int]] with ResultBindings {

  var nextResultArrayIndex: Int = 0
  var currentResultArray: Array[Array[Int]] = null

  val incomingResultsQueue = new LinkedBlockingQueue[Array[Array[Int]]]()

  def add(a: Array[Array[Int]]): Unit = {
    incomingResultsQueue.put(a)
  }

  def next: Array[Int] = {
    val result = currentResultArray(nextResultArrayIndex)
    nextResultArrayIndex += 1
    if (nextResultArrayIndex >= currentResultArray.length) {
      currentResultArray = null
    }
    result
  }

  @inline private[this] def replenishCurrentArray: Unit = {
    currentResultArray = incomingResultsQueue.take
    nextResultArrayIndex = 0
  }

  def hasNext: Boolean = {
    if (currentResultArray == null) {
      replenishCurrentArray
    }
    currentResultArray.length > 0
  }

}
