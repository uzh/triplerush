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

class SingleNodeTripleMapper(val numberOfNodes: Int, val workersPerNode: Int) extends VertexToWorkerMapper[Long] {
  if (numberOfNodes != 1) {
    throw new Exception("This triple mapper should only be used in single-instance TripleRush.")
  }

  val numberOfWorkers = numberOfNodes * workersPerNode

  def getWorkerIdForVertexId(vertexId: Long): Int = {
    val first = vertexId.extractFirst
    val second = vertexId.extractSecond
    ((first + second) & Int.MaxValue) % numberOfWorkers
  }

  def getWorkerIdForVertexIdHash(vertexIdHash: Int): Int = throw new UnsupportedOperationException("This mapper does not support mapping by vertex hash.")
}

object SingleNodeTripleMapperFactory extends MapperFactory[Long] {
  def createInstance(numberOfNodes: Int, workersPerNode: Int) = new SingleNodeTripleMapper(numberOfNodes, workersPerNode)
}
