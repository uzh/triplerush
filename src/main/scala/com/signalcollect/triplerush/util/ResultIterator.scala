package com.signalcollect.triplerush.util

import java.util.concurrent.locks.ReentrantLock

/**
 * Only supports a single result consumer thread.
 */
class ResultIterator extends Iterator[Array[Int]] {

  var allResultsReported = false

  var nextResultArrayIndex: Int = 0
  var currentResultArray: Array[Array[Int]] = null

  var listOfArraysOfArrays = List[Array[Array[Int]]]()

  def close {
    synchronized {
      allResultsReported = true
      notify
    }
  }

  def add(a: Array[Array[Int]]) {
    synchronized {
      listOfArraysOfArrays = a :: listOfArraysOfArrays
      notify
    }
  }

  def next: Array[Int] = {
    if (currentResultArray == null) {
      replenishCurrentArray
    }
    val result = currentResultArray(nextResultArrayIndex)
    nextResultArrayIndex += 1
    if (nextResultArrayIndex >= currentResultArray.length) {
      currentResultArray = null
    }
    result
  }

  private def replenishCurrentArray {
    synchronized {
      while (listOfArraysOfArrays == Nil) {
        wait
      }
      currentResultArray = listOfArraysOfArrays.head
      listOfArraysOfArrays = listOfArraysOfArrays.tail
      nextResultArrayIndex = 0
    }
  }

  def hasNext: Boolean = {
    if (currentResultArray != null || !listOfArraysOfArrays.isEmpty) {
      true
    } else {
      synchronized {
        while (!allResultsReported && listOfArraysOfArrays == Nil) {
          wait
        }
        if (allResultsReported && listOfArraysOfArrays == Nil) {
          false
        } else {
          true
        }
      }
    }
  }

}
