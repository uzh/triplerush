/*
 *  @author Philip Stutz
 *  @author Mihaela Verman
 *  
 *  Copyright 2013 University of Zurich
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

package com.signalcollect.triplerush

import com.signalcollect._
import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer

class QueryVertex(
  id: Int,
  val promise: Promise[(List[PatternQuery], Map[String, Any])],
  val expectedTickets: Long) extends ProcessingVertex[Int, PatternQuery](id) {
  var receivedTickets: Long = 0
  var firstResultNanoTime = 0l
  var complete = true

  override def shouldProcess(query: PatternQuery): Boolean = {
    receivedTickets += query.tickets
    complete &&= query.isComplete
    if (query.unmatched.isEmpty) {
      // Query was matched successfully.
      if (firstResultNanoTime == 0) {
        firstResultNanoTime = System.nanoTime
      }
      true
    } else {
      false
    }
  }

  override def scoreSignal: Double = if (expectedTickets == receivedTickets) 1 else 0

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    promise success (state, Map("firstResultNanoTime" -> firstResultNanoTime))
    graphEditor.removeVertex(id)
  }

  def process(item: PatternQuery, graphEditor: GraphEditor[Any, Any]) {}

}