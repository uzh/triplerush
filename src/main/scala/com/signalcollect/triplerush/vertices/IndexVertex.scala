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

import com.signalcollect.Edge
import com.signalcollect.GraphEditor
import com.signalcollect.interfaces.Inspectable
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.PlaceholderEdge
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.CardinalityRequest
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.triplerush.QueryParticle
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.ChildIdRequest
import com.signalcollect.triplerush.ChildIdRequest
import com.signalcollect.triplerush.CardinalityAndEdgeCountReply
import com.signalcollect.triplerush.TriplePattern

/**
 * This vertex represents part of the TripleRush index.
 */
abstract class IndexVertex(val id: TriplePattern)
  extends BaseVertex[TriplePattern, Any, Any]
  with ParentBuilding[Any, Any] {

  def foreachChildDelta(f: Int => Unit)

  def addChildDelta(delta: Int): Boolean

  def processQuery(query: Array[Int], graphEditor: GraphEditor[Any, Any])

  def handleCardinalityIncrement(i: Int) = {}

  def cardinality: Int

  /**
   * Default reply, is only overridden by SOIndex.
   */
  def handleCardinalityRequest(c: CardinalityRequest, graphEditor: GraphEditor[Any, Any]) {
    //TODO: add test case for fully bound pattern in a query that has at least one variable
    graphEditor.sendSignal(CardinalityReply(
      c.forPattern, cardinality), c.requestor, None)
  }

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val placeholderEdge = e.asInstanceOf[PlaceholderEdge]
    val wasAdded = addChildDelta(placeholderEdge.childDelta)
    wasAdded
  }

  def handleChildIdRequest(requestor: Any, graphEditor: GraphEditor[Any, Any])

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) = {
    signal match {
      case query: Array[Int] =>
        processQuery(query, graphEditor)
      case cr: CardinalityRequest =>
        handleCardinalityRequest(cr, graphEditor)
      case ChildIdRequest(requestor) =>
        handleChildIdRequest(requestor, graphEditor)
      case cardinalityIncrement: Int =>
        handleCardinalityIncrement(cardinalityIncrement)
      case other => throw new Exception(s"Unexpected signal @ $id: $other")
    }
    true
  }

}
