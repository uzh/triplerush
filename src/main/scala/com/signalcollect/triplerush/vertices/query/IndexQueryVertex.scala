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

import scala.concurrent.Promise
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.ChildIdRequest
import com.signalcollect.triplerush.ChildIdReply
import com.signalcollect.triplerush.vertices.BaseVertex

final class IndexQueryVertex(
  val indexId: Long,
  val resultPromise: Promise[Array[Int]]) extends BaseVertex[Nothing] {

  val id = QueryIds.embedQueryIdInLong(QueryIds.nextQueryId)

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    graphEditor.sendSignal(ChildIdRequest(id), indexId)
  }

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]): Boolean = {
    signal match {
      case ChildIdReply(intSet) =>
        resultPromise.success(intSet)
        graphEditor.removeVertex(id)
    }
    true
  }

}
