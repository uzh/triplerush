package com.signalcollect.triplerush.util

class SequenceOfArraysTraversable extends Traversable[Array[Int]] with ResultBindings {

  var listOfArraysOfArrays = List[Seq[Array[Int]]]()

  def add(a: Seq[Array[Int]]) {
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
