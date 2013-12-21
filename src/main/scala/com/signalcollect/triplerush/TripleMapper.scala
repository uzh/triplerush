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

class TripleMapper[Id](val numberOfNodes: Int, val workersPerNode: Int) extends VertexToWorkerMapper[Id] {
  val numberOfWorkers = numberOfNodes * workersPerNode

  def getWorkerIdForVertexId(vertexId: Id): Int = {
    vertexId match {
      //      case tp: TriplePattern => {
      //        // Check diagram in paper. The goal is to have binding index vertices placed on the same node as
      //        // their respective parent index vertex. At the same time index vertices should be load balanced over all workers on a node.
      //        if (tp.s > 0 && tp.o == 0) {
      //          // Leftmost on figure.
      //          workerId(nodeAssignmentId = tp.s, nodeBalanceId = tp.p)
      //        } else if (tp.o > 0 && tp.p == 0) {
      //          // Rightmost on figure.
      //          workerId(nodeAssignmentId = tp.o, nodeBalanceId = tp.s)
      //        } else {
      //          // Middle on figure.
      //          workerId(nodeAssignmentId = tp.p, nodeBalanceId = tp.o)
      //        }
      //      }
      case tp: TriplePattern => {
        val s = tp.s
        if (s > 0) {
          s % numberOfWorkers
        } else if (tp.o > 0) {
          tp.o % numberOfWorkers
        } else if (tp.p > 0) {
          tp.p % numberOfWorkers
        } else {
          // Put it on the last node, so it does not collide with the node which has the coordinator.
          // Put it on the 1st worker there.
          workerId(nodeAssignmentId = numberOfNodes - 1, nodeBalanceId = 1)
        }
      }
      case qv: Int => loadBalance(qv, numberOfWorkers)
      case other => throw new UnsupportedOperationException("This mapper does not support mapping ids of type " + other.getClass)
    }
  }

  def workerId(nodeAssignmentId: Int, nodeBalanceId: Int): Int = {
    val nodeId = loadBalance(nodeAssignmentId, numberOfNodes)
    val workerOnNode = loadBalance(nodeAssignmentId + nodeBalanceId, workersPerNode)
    nodeId * workersPerNode + workerOnNode
  }

  def loadBalance(i: Int, slots: Int) = {
    val preliminarySlot = i % slots
    if (preliminarySlot < 0) {
      if (preliminarySlot == Int.MinValue) {
        // Special case,-Int.MinValue == Int.MinValue
        0
      } else {
        -preliminarySlot
      }
    } else {
      preliminarySlot
    }
  }

  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
}

object TripleMapperFactory extends MapperFactory {
  def createInstance[Id](numberOfNodes: Int, workersPerNode: Int) = new TripleMapper[Id](numberOfNodes, workersPerNode)
}