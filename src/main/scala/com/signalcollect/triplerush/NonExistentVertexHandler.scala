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

import com.signalcollect.GraphEditor
import QueryParticle.arrayToParticle
import com.signalcollect.Vertex
import com.signalcollect.Edge
import com.signalcollect.triplerush.vertices._

case object NonExistentVertexHandler {
  def createIndexVertex(edge: Edge[Any], vertexId: Any): Option[Vertex[Any, _]] = {
    vertexId match {
      case TriplePattern(0, 0, 0) => throw new Exception("Root vertex does not exist, should always be added first.")
      case tp @ TriplePattern(0, 0, o) => Some(new OIndex(tp))
      case tp @ TriplePattern(0, p, 0) => Some(new PIndex(tp))
      case tp @ TriplePattern(s, 0, 0) => Some(new SIndex(tp))
      case tp @ TriplePattern(s, p, 0) => Some(new SPIndex(tp))
      case tp @ TriplePattern(s, 0, o) => Some(new SOIndex(tp))
      case tp @ TriplePattern(0, p, o) => Some(new POIndex(tp))
      case other => throw new Exception(s"Could not add edge $edge to vertex $vertexId, because that vertex does not exist.")
    }
  }
}
