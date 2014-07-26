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

package com.signalcollect.triplerush.vertices.query

import scala.concurrent.Promise
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.TriplePattern

/**
 * If execution is complete returns Some(numberOfResults), else returns None.
 */
final class ResultCountingQueryVertex(
  query: Seq[TriplePattern],
  tickets: Long,
  resultPromise: Promise[Option[Long]],
  optimizer: Option[Optimizer])
  extends AbstractQueryVertex[Long](query, tickets, numberOfSelectVariables = 0, optimizer) {

  val id = QueryIds.embedQueryIdInLong(QueryIds.nextCountQueryId)

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    state = 0
    super.afterInitialization(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]]) {
    throw new UnsupportedOperationException("Result counting vertex should never receive bindings.")
  }

  def handleResultCount(resultCount: Long) {
    state += resultCount
  }

  override def reportResults {
    if (!resultsReported) {
      super.reportResults
      if (complete) {
        resultPromise.success(Some(state))
      } else {
        resultPromise.success(None)
      }
    }
  }

}
