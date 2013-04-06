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