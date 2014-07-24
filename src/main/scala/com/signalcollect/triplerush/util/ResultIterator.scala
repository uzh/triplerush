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

  var incomingResultsQueue = new LinkedBlockingQueue[Array[Array[Int]]]()

  def add(a: Array[Array[Int]]) {
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

  private def replenishCurrentArray {
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
