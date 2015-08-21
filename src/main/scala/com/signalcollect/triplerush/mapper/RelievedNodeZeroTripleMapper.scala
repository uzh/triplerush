/*
 *  @author Philip Stutz
 *
 *  Copyright 2015 iHealth Technologies
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

package com.signalcollect.triplerush.mapper

import com.signalcollect.interfaces.MapperFactory

/**
 * This is a distributed triple mapper that places no index vertices on node 0.
 */
final class RelievedNodeZeroTripleMapper(numberOfNodes: Int, workersPerNode: Int)
    extends DistributedTripleMapper(numberOfNodes, workersPerNode) {
  assert(numberOfNodes >= 2, "RelievedNodeZeroTripleMapper can only be used on a cluster with more than one node.")
  val numberOfIndexNodes = numberOfNodes - 1 // No index vertices on node 0.

  /**
   * Asserts that both nodeAssignmentId and nodeBalanceId
   * are larger than or equal to zero.
   */
  @inline override def workerIdOptimized(nodeAssignmentId: Int, workerAssignmentId: Int): Int = {
    val nodeId = (nodeAssignmentId % numberOfIndexNodes) + 1 // No index vertices on node 0. 
    val workerOnNode = workerAssignmentId % workersPerNode
    nodeId * workersPerNode + workerOnNode
  }

}

object RelievedNodeZeroTripleMapperFactory extends MapperFactory[Long] {
  def createInstance(numberOfNodes: Int, workersPerNode: Int) = new RelievedNodeZeroTripleMapper(numberOfNodes, workersPerNode)
}
