package com.signalcollect.triplerush.util

import java.util.concurrent.locks.ReentrantLock

/**
 * Only supports a single result consumer thread.
 */
final class ResultIterator extends Iterator[Array[Int]] with ResultBindings {

  var allResultsReported = false

  var nextResultArrayIndex: Int = 0
  var currentResultArray: Seq[Array[Int]] = null

  var listOfArraysOfArrays = List[Seq[Array[Int]]]()

  def close {
    synchronized {
      allResultsReported = true
      notify
    }
  }

  def add(a: Seq[Array[Int]]) {
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
