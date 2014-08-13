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

class LoadBalancingTripleMapper(val numberOfNodes: Int, val workersPerNode: Int) extends VertexToWorkerMapper[Long] {

  val numberOfWorkers = numberOfNodes * workersPerNode

  def getWorkerIdForVertexId(vertexId: Long): Int = {
    val first = vertexId.extractFirst
    val second = vertexId.extractSecond
    val loadBalanceId = finalizeHash(mixLast(first, second), 3) & Int.MaxValue
    if (first < 0) {
      if (second < 0) {
        // It's a query id, map to first node and load balance on the workers there.
        loadBalanceId % workersPerNode
      } else {
        // First encodes a predicate, second encodes an object.
        if (second > 0) {
          // Object is not a wildcard and we use it for node assignment.
          workerIdOptimized(nodeAssignmentId = second, workerAssignmentId = loadBalanceId)
        } else {
          // Everything but the predicate is a wildcard. We use the predicate for both node assignment and load balancing. 
          val p = first & Int.MaxValue
          workerIdOptimized(nodeAssignmentId = p, workerAssignmentId = loadBalanceId)
        }
      }
    } else if (first > 0) {
      // First represents the subject and we use it for node assignment.
      workerIdOptimized(nodeAssignmentId = first, workerAssignmentId = loadBalanceId)
    } else {
      // Subject is a wildcard, we use whatever is in second for node assignment.
      val predicateOrObject = second & Int.MaxValue
      workerIdOptimized(nodeAssignmentId = predicateOrObject, workerAssignmentId = loadBalanceId)
    }
  }

  /**
   * Asserts that both nodeAssignmentId and nodeBalanceId
   * are larger than or equal to zero.
   */
  @inline final def workerIdOptimized(nodeAssignmentId: Int, workerAssignmentId: Int): Int = {
    val nodeId = nodeAssignmentId % numberOfNodes
    val workerOnNode = workerAssignmentId % workersPerNode
    nodeId * workersPerNode + workerOnNode
  }

  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
}

object LoadBalancingTripleMapperFactory extends MapperFactory[Long] {
  def createInstance(numberOfNodes: Int, workersPerNode: Int) = new LoadBalancingTripleMapper(numberOfNodes, workersPerNode)
}
