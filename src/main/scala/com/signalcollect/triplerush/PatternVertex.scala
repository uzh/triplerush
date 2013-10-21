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

import com.signalcollect.GraphEditor

/**
 * Basic vertex that recursively builds the TripleRush index structure.
 */
abstract class PatternVertex[Signal, State](
  id: TriplePattern)
  extends BaseVertex[TriplePattern, Signal, State](id) {

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index on initialization.
    id.parentPatterns foreach { parentId =>
      if (parentId != RootPattern) {
        // The root is added initially, no need to add again.
        val indexVertex = parentId match {
          case TriplePattern(s, 0, 0) => new SIndexVertex(parentId)
          case TriplePattern(0, p, 0) => new PIndexVertex(parentId)
          case TriplePattern(0, 0, o) => new OIndexVertex(parentId)
        }
        graphEditor.addVertex(indexVertex)
        // TODO: Add handling for root index vertex.
        val idDelta = id.parentIdDelta(parentId)
        graphEditor.addEdge(parentId, new PlaceholderEdge(idDelta))
      }
    }
  }
}
