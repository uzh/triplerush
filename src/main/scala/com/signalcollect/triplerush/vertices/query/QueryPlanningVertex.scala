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
import com.signalcollect.triplerush.QueryIds
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.PredicateStatsCache

/**
 * All times in nanoseconds.
 */
case class QueryPlanningResult(
  queryPlan: Seq[TriplePattern],
  totalPlanningDuration: Long,
  statsGatheringTime: Long,
  actualOptimizerTime: Long)

/**
 * Query vertex that only does the query planning and returns the query plan.
 * The plan is *NOT* executed.
 */
class QueryPlanningVertex(
  query: Seq[TriplePattern],
  plannerPromise: Promise[QueryPlanningResult],
  optimizer: Optimizer)
  extends AbstractQueryVertex[ArrayOfArraysTraversable](query, 0l, 0, Some(optimizer)) {

  final val id = QueryIds.embedQueryIdInLong(QueryIds.nextQueryId)

  var queryPlan: Seq[TriplePattern] = _
  var statsGatheringStartingTime = -1l
  var totalPlanningDuration = -1l
  var statsGatheringTime = -1l
  var actualOptimizerTime = -1l

  override final def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    optimizingStartTime = System.nanoTime
    if (numberOfPatternsInOriginalQuery > 1) {
      statsGatheringStartingTime = System.nanoTime
      gatherStatistics(graphEditor)
    } else {
      totalPlanningDuration = System.nanoTime - optimizingStartTime
      statsGatheringTime = 0l
      actualOptimizerTime = 0l
      queryPlan = query
      reportResultsAndRequestQueryVertexRemoval(graphEditor)
    }
  }

  override def handleQueryDispatch(graphEditor: GraphEditor[Long, Any]) {
    statsGatheringTime = System.nanoTime - statsGatheringStartingTime
    val actualOptimizationStartingTime = System.nanoTime
    queryPlan = optimizer.optimize(
      cardinalities, PredicateStatsCache.implementation)
    actualOptimizerTime = System.nanoTime - actualOptimizationStartingTime
    totalPlanningDuration = System.nanoTime - optimizingStartTime
    reportResultsAndRequestQueryVertexRemoval(graphEditor)
  }

  def handleBindings(bindings: Array[Array[Int]]) {
    throw new UnsupportedOperationException("Query planning vertex should never receive a binding.")
  }

  def handleResultCount(resultCount: Long) {
    throw new UnsupportedOperationException("Query planning vertex should never receive a result count.")
  }

  override final def reportResults {
    plannerPromise.success(QueryPlanningResult(queryPlan, totalPlanningDuration, statsGatheringTime, actualOptimizerTime))
  }

}
