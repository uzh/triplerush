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

package com.signalcollect.triplerush

import com.signalcollect.interfaces.VertexToWorkerMapper
import com.signalcollect.interfaces.MapperFactory
import com.signalcollect.triplerush.EfficientIndexPattern._
import scala.util.hashing.MurmurHash3._

class AlternativeTripleMapper(val numberOfNodes: Int, val workersPerNode: Int, predicateIndexVerticesOnNode0: Boolean) extends VertexToWorkerMapper[Long] {

  val numberOfWorkers = numberOfNodes * workersPerNode

  val modForPredicates = if (predicateIndexVerticesOnNode0) {
    workersPerNode
  } else {
    numberOfWorkers
  }

  def getWorkerIdForVertexId(vertexId: Long): Int = {
    val first = vertexId.extractFirst
    if (first > 0) {
      first % numberOfWorkers
    } else {
      val second = vertexId.extractSecond
      if (second > 0) {
        second % numberOfWorkers
      } else if (first < 0 && second < 0) {
        // It's a query ID, we need to put it on node 0.
        ((first + second) & Int.MaxValue) % workersPerNode
      } else {
        // Only a predicate is set, the other two are wildcards. This means that the predicate is stored in first.
        (first & Int.MaxValue) % modForPredicates
      }
    }
  }

  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
}

class AlternativeTripleMapperFactory(predicateNodesOnNode0: Boolean) extends MapperFactory[Long] {
  def createInstance(numberOfNodes: Int, workersPerNode: Int) = new AlternativeTripleMapper(numberOfNodes, workersPerNode, predicateNodesOnNode0)
}
