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

package com.signalcollect.triplerush.vertices

import com.signalcollect._

/**
 * Base implementation for all TripleRush vertices.
 */
abstract class BaseVertex[Id, SignalType, State]
  extends Vertex[Id, State] {

  @transient var state: State = _

  def setState(s: State) {
    state = s
  }

  override def scoreCollect = 0
  override def scoreSignal = 0
  override def toString = s"${this.getClass.getName}(id=$id)"
  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) = throw new Exception("Should not be called, signals on delivery.")
  override def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) = throw new Exception("Should not be called, collects and signals on delivery.")
  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {}
  override def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}
  override def edgeCount = 0
  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = throw new UnsupportedOperationException
  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = 0
}
