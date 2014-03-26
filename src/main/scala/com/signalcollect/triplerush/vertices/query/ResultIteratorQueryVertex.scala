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

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.util.ResultIterator

final class ResultIteratorQueryVertex(
  querySpecification: QuerySpecification,
  resultIterator: ResultIterator,
  optimizer: Option[Optimizer])

  extends AbstractQueryVertex[ResultIterator](querySpecification, optimizer) {

  val id = QueryIds.nextQueryId

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    state = resultIterator
    super.afterInitialization(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]]) {
    state.add(bindings)
  }

  def handleResultCount(resultCount: Long) {
    throw new UnsupportedOperationException("Result binding vertex should never receive a result count.")
  }

  override def reportResults {
    if (!resultsReported) {
      super.reportResults
      state.close
    }
  }

}
