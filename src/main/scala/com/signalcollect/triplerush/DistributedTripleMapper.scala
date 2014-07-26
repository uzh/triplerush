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
import scala.util.hashing.MurmurHash3._

class DistributedTripleMapper(val numberOfNodes: Int, val workersPerNode: Int) extends VertexToWorkerMapper[Long] {
  val numberOfWorkers = numberOfNodes * workersPerNode

  def getWorkerIdForVertexId(vertexId: Long): Int = {
    /**
     * Try to map things with to a node based on the subject/object, wherever possible.
     * Load balance over the workers of that node by using other unused triple information.
     */
    if (vertexId.isQueryId) {
      // Avoid issues with mod on negative numbers by cutting off the 2s complement 1 at the front.
      // This guarantees a 'positive' outcome. :)
      // Always puts query vertices on node 0.
      (QueryIds.extractQueryIdFromLong(vertexId) & Int.MaxValue) % workersPerNode
    } else {
      // Duplicated code, we don't want to create any new objects here.
      val first = vertexId.extractFirst
      val second = vertexId.extractSecond
      val s = math.max(0, first)
      val o = math.max(0, second)
      val p = if (first < 0) {
        first & Int.MaxValue
      } else {
        if (second < 0) { // second < 0
          second & Int.MaxValue
        } else {
          0
        }
      }
      val loadBalanceId = finalizeHash(mixLast(first, second), 3) & Int.MaxValue
      if (s > 0) {
        workerIdOptimized(nodeAssignmentId = s, nodeLoadBalanceId = loadBalanceId)
      } else if (o > 0) {
        workerIdOptimized(nodeAssignmentId = o, nodeLoadBalanceId = loadBalanceId)
      } else if (p > 0) {
        workerIdOptimized(nodeAssignmentId = p, nodeLoadBalanceId = loadBalanceId)
      } else {
        // Root, put it on the last node, so it does not collide with the node which has the coordinator, when there are multiple nodes.
        // Put it on the second worker there.
        workerIdOptimized(nodeAssignmentId = numberOfNodes - 1, nodeLoadBalanceId = 1)
      }
    }
  }

  /**
   * Asserts that both nodeAssignmentId and nodeBalanceId
   * are larger than or equal to zero.
   */
  def workerIdOptimized(nodeAssignmentId: Int, nodeLoadBalanceId: Int): Int = {
    val nodeId = nodeAssignmentId % numberOfNodes
    val workerOnNode = nodeLoadBalanceId % workersPerNode
    nodeId * workersPerNode + workerOnNode
  }

  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
}

object DistributedTripleMapperFactory extends MapperFactory[Long] {
  def createInstance(numberOfNodes: Int, workersPerNode: Int) = new DistributedTripleMapper(numberOfNodes, workersPerNode)
}
