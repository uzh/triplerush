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

package com.signalcollect.triplerush.optimizers

import scala.collection.mutable.ArrayBuffer
import com.signalcollect.triplerush.TriplePattern
import scala.Array.canBuildFrom
import com.signalcollect.triplerush.PredicateStats

object CleverCardinalityOptimizer extends Optimizer {

  def optimize(cardinalities: Map[TriplePattern, Long], predicateStats: Map[Int, PredicateStats]): Array[TriplePattern] = {
    var sortedPatterns = cardinalities.toArray.sortBy(_._2)
    var boundVariables = Set[Int]() // The lower the score, the more constrained the variable.
    val optimizedPatterns = ArrayBuffer[TriplePattern]()
    while (!sortedPatterns.isEmpty) {
      val nextPattern = sortedPatterns.head._1
      optimizedPatterns.append(nextPattern)
      val variablesInPattern = nextPattern.variables
      for (variable <- variablesInPattern) {
        boundVariables += variable
      }
      sortedPatterns = sortedPatterns.tail.map {
        case (pattern, oldCardinalityEstimate) =>
          // We don't care about the old estimate.
          var cardinalityEstimate = cardinalities(pattern).toDouble
          var foundUnbound = false
          for (variable <- pattern.variables) {
            if (boundVariables.contains(variable)) {
              cardinalityEstimate = cardinalityEstimate / 100.0
            } else {
              foundUnbound = true
            }
          }
          if (!foundUnbound) {
            cardinalityEstimate = 1.0 + cardinalityEstimate / 100000000
          }
          (pattern, cardinalityEstimate.toLong)
      }
      sortedPatterns = sortedPatterns sortBy (_._2)
    }
    optimizedPatterns.toArray
  }

}
