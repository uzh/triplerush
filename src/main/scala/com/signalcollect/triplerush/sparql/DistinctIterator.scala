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

package com.signalcollect.triplerush.sparql

import com.signalcollect.triplerush.util.ResultBindingsHashSet

class DistinctIterator(encodedResultIterator: Iterator[Array[Int]]) extends Iterator[Array[Int]] {

  val alreadyReportedBindings = new ResultBindingsHashSet(128)

  var distinctNext: Array[Int] = if (encodedResultIterator.hasNext) {
    val next = encodedResultIterator.next
    alreadyReportedBindings.add(next)
    next
  } else {
    null.asInstanceOf[Array[Int]]
  }

  def hasNext: Boolean = {
    distinctNext != null
  }

  def next: Array[Int] = {
    val nextThatWeWillReport = distinctNext
    distinctNext = null.asInstanceOf[Array[Int]]
    // Refill the next slot.
    while (distinctNext == null && encodedResultIterator.hasNext) {
      val next = encodedResultIterator.next
      val alreadyReported = alreadyReportedBindings.add(next)
      if (!alreadyReported) {
        distinctNext = next
      }
    }
    nextThatWeWillReport
  }
}