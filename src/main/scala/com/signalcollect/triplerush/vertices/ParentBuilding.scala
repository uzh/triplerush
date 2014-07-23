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

import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.PlaceholderEdge

/**
 * Basic vertex that recursively builds the TripleRush index structure.
 */
trait ParentBuilding[State] extends BaseVertex[State] {

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    // Build the hierarchical index on initialization.
    id.parentIds foreach { parentId =>
      val idDelta = id.parentIdDelta(parentId)
      graphEditor.addEdge(parentId, new PlaceholderEdge(idDelta))
    }
  }

}
