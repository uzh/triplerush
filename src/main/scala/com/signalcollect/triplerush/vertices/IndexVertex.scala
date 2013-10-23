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

/**
 * This vertex represents part of the TripleRush index.
 */
abstract class IndexVertex(id: TriplePattern)
  extends BaseVertex[TriplePattern, Any, Any](id)
  with ParentBuilding[Any, Any]
  with Inspectable[TriplePattern, Any] {

  def foreachChildDelta[U](f: Int => U)

  def addChildDelta(delta: Int): Boolean

  def processQuery(query: Array[Int], graphEditor: GraphEditor[Any, Any])

  def handleCardinalityIncrement(i: Int) = {}

  def cardinality: Int

  override def targetIds: Traversable[TriplePattern] = {
    val buffer = new ArrayBuffer[TriplePattern]
    foreachChildDelta(delta => buffer.append(id.childPatternRecipe(delta)))
    buffer
  }

  override def expose: Map[String, Any] = {
    var extraInfo: Map[String, Any] = Map("outDegree" -> edgeCount)
    extraInfo ++= targetIds take 10 map { targetId => "outEdge sample" -> targetId }
    extraInfo
  }

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    val placeholderEdge = e.asInstanceOf[PlaceholderEdge]
    val wasAdded = addChildDelta(placeholderEdge.childDelta)
    wasAdded
  }

  override def deliverSignal(signal: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) = {
    signal match {
      case query: Array[Int] =>
        val q = new QueryParticle(query)
        println(s"${q.queryId} @$id")
        //        println(s"Query(${q.queryId} @ $id with cardinality $cardinality and #edges $edgeCount):\n" +
        //          s"Last pattern: ${q.lastPattern.lookup}\n" +
        //          s"#Existing bindings: ${q.bindings.mkString(", ")}")
        processQuery(query, graphEditor)
      case CardinalityRequest(id, requestor: AnyRef) =>
        graphEditor.sendSignal(CardinalityReply(id, cardinality), requestor, None)
      case cardinalityIncrement: Int =>
        handleCardinalityIncrement(cardinalityIncrement)
      case other => println(s"WTF: $other")
    }
    true
  }

}
