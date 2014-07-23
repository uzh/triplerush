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
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable
import com.signalcollect.triplerush.TriplePattern

class ResultBindingQueryVertex(
  query: Seq[TriplePattern],
  numberOfSelectVariables: Int,
  tickets: Long,
  resultPromise: Promise[Traversable[Array[Int]]],
  statsPromise: Promise[Map[Any, Any]],
  optimizer: Option[Optimizer])
  extends AbstractQueryVertex[ArrayOfArraysTraversable](query, tickets, numberOfSelectVariables, optimizer) {
   
  final val id = QueryIds.embedQueryIdInLong(QueryIds.nextQueryId)
  
  override final def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    state = new ArrayOfArraysTraversable
    super.afterInitialization(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]]) {
    state.add(bindings)
  }

  def handleResultCount(resultCount: Long) {
    throw new UnsupportedOperationException("Result binding vertex should never receive a result count.")
  }

  override final def reportResults {
    if (!resultsReported) {
      super.reportResults
      resultPromise.success(state)
      val stats = Map[Any, Any](
        "isComplete" -> complete,
        "optimizingDuration" -> optimizingDuration,
        "optimizedQuery" -> ("Pattern matching order: " + {
          if (dispatchedQuery.isDefined) {
            new QueryParticle(dispatchedQuery.get).patterns.toList
          } else { "Inferred that the query has no results before it was dispatched." }
        })).withDefaultValue("")
      statsPromise.success(stats)
    }
  }

}
