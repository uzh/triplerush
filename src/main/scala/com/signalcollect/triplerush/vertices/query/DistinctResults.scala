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

package com.signalcollect.triplerush.vertices.query

import com.signalcollect.triplerush.util.ResultBindingWrapper
import com.signalcollect.triplerush.util.ResultBindingsHashSet
import com.signalcollect.triplerush.util.ResultBindings

trait DistinctResults[State <: ResultBindings] extends {
  this: AbstractQueryVertex[State] =>

  val alreadyReportedBindings = new ResultBindingsHashSet(128)

  override def handleBindings(bindings: Array[Array[Int]]) {
    var toReport = Vector[Array[Int]]()
    var i = 0
    val length = bindings.length
    while (i < length) {
      val currentBindings = bindings(i)
      val alreadyReported = alreadyReportedBindings.add(currentBindings)
      if (!alreadyReported) {
        toReport = toReport :+ currentBindings
      }
      i += 1
    }
    state.add(toReport)
  }

}
