package com.signalcollect.triplerush.util

class ArrayOfArraysTraversable extends Traversable[Array[Int]] {

  var listOfArraysOfArrays = List[Array[Array[Int]]]()

  def add(a: Array[Array[Int]]) {
    listOfArraysOfArrays = a :: listOfArraysOfArrays
  }

  def foreach[U](f: Array[Int] => U): Unit = {
    for (arrayOfArrays <- listOfArraysOfArrays) {
      for (array <- arrayOfArrays) {
        f(array)
      }
    }
  }

}
