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
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable

final class ResultBindingQueryVertex(
  querySpecification: QuerySpecification,
  resultPromise: Promise[Traversable[Array[Int]]],
  statsPromise: Promise[Map[Any, Any]],
  optimizer: Option[Optimizer])
  
  extends AbstractQueryVertex[ArrayOfArraysTraversable](querySpecification, optimizer) {

  val id = QueryIds.nextQueryId
  var queryCopyCount = 0l

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    super.afterInitialization(graphEditor)
    state = new ArrayOfArraysTraversable
  }

  def handleBindings(bindings: Array[Array[Int]]) {
    queryCopyCount += 1
    state.add(bindings)
  }

  def handleResultCount(resultCount: Long) {
    throw new UnsupportedOperationException("Result binding vertex should never receive a result count.")
  }

  override def queryDone(graphEditor: GraphEditor[Any, Any]) {
    if (!isQueryDone) {
      resultPromise.success(state)
      val stats = Map[Any, Any](
        "isComplete" -> complete,
        "optimizingDuration" -> optimizingDuration,
        "queryCopyCount" -> queryCopyCount,
        "optimizedQuery" -> ("Pattern matching order: " + {
          if (dispatchedQuery.isDefined) {
            new QueryParticle(dispatchedQuery.get).patterns.toList + "\nCardinalities: " + cardinalities.toString
          } else { "the optimizer was not run, probably one of the patterns had cardinality 0" }
        })).withDefaultValue("")
      statsPromise.success(stats)
      super.queryDone(graphEditor)
    }
  }

}
