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

import com.signalcollect.interfaces.VertexToWorkerMapper
import com.signalcollect.interfaces.MapperFactory
import com.signalcollect.triplerush.EfficientIndexPattern._

class TripleMapper(val numberOfNodes: Int, val workersPerNode: Int) extends VertexToWorkerMapper[Long] {
  val numberOfWorkers = numberOfNodes * workersPerNode

  def getWorkerIdForVertexId(vertexId: Long): Int = {
    vertexId match {
      /**
       * Try to map things with to a node based on the subject/object, wherever possible.
       * Load balance over the workers of that node by using other unused triple information.
       */
      case indexPatternOrQueryVertex: Long =>
        if (indexPatternOrQueryVertex.isQueryId) {
          // Avoid issues with mod on negative numbers by cutting off the 2s complement 1 at the front.
          // This guarantees a 'positive' outcome. :)
          // Always puts query vertices on node 0.
          (QueryIds.extractQueryIdFromLong(indexPatternOrQueryVertex) & Int.MaxValue) % workersPerNode
        } else {
          val s = indexPatternOrQueryVertex.s
          val p = indexPatternOrQueryVertex.p
          val o = indexPatternOrQueryVertex.o
          if (s > 0) {
            if (p > 0) {
              workerIdOptimized(nodeAssignmentId = s, nodeBalanceId = s + p)
            } else if (o > 0) {
              workerIdOptimized(nodeAssignmentId = s, nodeBalanceId = s + o)
            } else {
              workerIdOptimized(nodeAssignmentId = s, nodeBalanceId = s)
            }
          } else if (o > 0) {
            if (p > 0) {
              workerIdOptimized(nodeAssignmentId = o, nodeBalanceId = o + p)
            } else {
              workerIdOptimized(nodeAssignmentId = o, nodeBalanceId = o)
            }
          } else if (p > 0) {
            workerIdOptimized(nodeAssignmentId = p, nodeBalanceId = p)
          } else {
            // Root, put it on the last node, so it does not collide with the node which has the coordinator, when there are multiple nodes.
            // Put it on the second worker there.
            workerIdOptimized(nodeAssignmentId = numberOfNodes - 1, nodeBalanceId = 1)
          }
        }
      case other => throw new UnsupportedOperationException("This mapper does not support mapping ids of type " + other.getClass)
    }
  }

  /**
   * Asserts that both nodeAssignmentId and nodeBalanceId
   * are larger than or equal to zero.
   */
  def workerIdOptimized(nodeAssignmentId: Int, nodeBalanceId: Int): Int = {
    val nodeId = nodeAssignmentId % numberOfNodes
    val workerOnNode = nodeBalanceId % workersPerNode
    nodeId * workersPerNode + workerOnNode
  }

  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
}

object TripleMapperFactory extends MapperFactory[Long] {
  def createInstance(numberOfNodes: Int, workersPerNode: Int) = new TripleMapper(numberOfNodes, workersPerNode)
}