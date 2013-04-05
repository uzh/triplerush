package com.signalcollect.triplerush

import com.signalcollect._

class TripleVertex(id: TriplePattern) extends PatternVertex(id) {
  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  def process(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val boundQueryOption = query.bind(id)
    if (boundQueryOption.isDefined) {
      val boundQuery = boundQueryOption.get
      if (boundQuery.unmatched.isEmpty) {
        // Query successful, send to query vertex.
        graphEditor.sendSignal(boundQuery, boundQuery.queryId, None)
      } else {
        // Query not complete yet, route onwards.
        graphEditor.sendSignal(boundQuery, boundQuery.unmatched.head.routingAddress, None)
      }
    } else {
      // Failed to bind, send to query vertex.
      graphEditor.sendSignal(query, query.queryId, None)
    }
  }
}