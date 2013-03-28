package com.signalcollect.triplerush

import com.signalcollect._
import scala.collection.mutable.ArrayBuffer

class TripleVertex(id: TriplePattern) extends PatternVertex(id) {
  /**
   * Binds the queries to the pattern of this vertex and routes them to their next destinations.
   */
  def signal(query: PatternQuery, graphEditor: GraphEditor[Any, Any]) {
    val boundQueryOption = query.bind(id)
    if (boundQueryOption.isDefined) {
      val boundQuery = boundQueryOption.get
      if (boundQuery.unmatched.isEmpty) {
        graphEditor.sendSignal(boundQuery, boundQuery.queryId, None)
      } else {
        val routingAddress = boundQuery.unmatched.head.routingAddress
        graphEditor.sendSignal(boundQuery, routingAddress, None)
      }
    }
  }
}
