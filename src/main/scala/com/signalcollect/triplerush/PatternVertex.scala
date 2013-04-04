package com.signalcollect.triplerush

import com.signalcollect._

/**
 * Basic vertex that recursively builds the TripleRush index structure. 
 */
abstract class PatternVertex(
  id: TriplePattern)
  extends ProcessingVertex[TriplePattern, PatternQuery](id) {

  override def afterInitialization(graphEditor: GraphEditor[Any, Any]) {
    // Build the hierarchical index on initialization.
    id.parentPatterns foreach { parentId =>
      graphEditor.addVertex(new IndexVertex(parentId))
      graphEditor.addEdge(parentId, new StateForwarderEdge(id))
    }
  }
}
