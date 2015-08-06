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

class ArrayOfArraysTraversable extends Traversable[Array[Int]] with ResultBindings {

  var listOfArraysOfArrays = List[Array[Array[Int]]]()

  def add(a: Array[Array[Int]]): Unit = {
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
