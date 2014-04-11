package com.signalcollect.triplerush.util

class ArrayOfArraysTraversable extends Traversable[Array[Int]] with ResultBindings {

  var listOfArraysOfArrays = List[Array[Array[Int]]]()

  def add(a: Array[Array[Int]]) {
    listOfArraysOfArrays = a :: listOfArraysOfArrays
  }

  override def size = listOfArraysOfArrays.foldLeft(0) {
    case (aggr, next) => aggr + next.length
  }

  def foreach[U](f: Array[Int] => U): Unit = {
    for (arrayOfArrays <- listOfArraysOfArrays) {
      for (array <- arrayOfArrays) {
        f(array)
      }
    }
  }

}
