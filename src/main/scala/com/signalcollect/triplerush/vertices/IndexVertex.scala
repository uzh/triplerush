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
import com.signalcollect.triplerush.CardinalityReply
import com.signalcollect.triplerush.CardinalityRequest
import com.signalcollect.triplerush.ChildIdRequest
import com.signalcollect.triplerush.ObjectCountSignal
import com.signalcollect.triplerush.PlaceholderEdge
import com.signalcollect.triplerush.SubjectCountSignal
import com.signalcollect.triplerush.TrGlobal
import com.signalcollect.triplerush.EfficientIndexPattern

/**
 * This vertex represents part of the TripleRush index.
 */
abstract class IndexVertex[State](val id: Long)
  extends BaseVertex[State]
  with ParentBuilding[State] {

  override def toString = {
    val indexType = getClass.getSimpleName
    val d = TrGlobal.dictionary
    val p = new EfficientIndexPattern(id)
    if (d.isDefined) {
      val sString = d.get(p.s)
      val pString = d.get(p.p)
      val oString = d.get(p.o)
      s"$indexType:\n\tSubject=$sString (ID=${p.s}),\n\tPredicate=$pString (ID=${p.p}),\n\tObject=$oString (ID=${p.o}))"
    } else {
      s"$indexType(\n\tSubjectID=${p.s},\n\tPredicateID=${p.p},\n\tObjectID=${p.o})"
    }
  }

  def foreachChildDelta(f: Int => Unit)

  def addChildDelta(delta: Int): Boolean

  def processQuery(query: Array[Int], graphEditor: GraphEditor[Long, Any])

  def handleCardinalityIncrement(i: Int) = {}

  def handleObjectCount(count: ObjectCountSignal) = {}

  def handleSubjectCount(count: SubjectCountSignal) = {}

  def cardinality: Int

  /**
   * Default reply, is only overridden by SOIndex.
   */
  def handleCardinalityRequest(c: CardinalityRequest, graphEditor: GraphEditor[Long, Any]) {
    graphEditor.sendSignal(CardinalityReply(
      c.forPattern, cardinality), c.requestor)
  }

  override def addEdge(e: Edge[Long], graphEditor: GraphEditor[Long, Any]): Boolean = {
    val placeholderEdge = e.asInstanceOf[PlaceholderEdge]
    val wasAdded = addChildDelta(placeholderEdge.childDelta)
    wasAdded
  }

  def handleChildIdRequest(requestor: Long, graphEditor: GraphEditor[Long, Any])

  override def deliverSignalWithoutSourceId(signal: Any, graphEditor: GraphEditor[Long, Any]) = {
    signal match {
      case query: Array[Int] =>
        processQuery(query, graphEditor)
      case cr: CardinalityRequest =>
        handleCardinalityRequest(cr, graphEditor)
      case ChildIdRequest(requestor) =>
        handleChildIdRequest(requestor, graphEditor)
      case cardinalityIncrement: Int =>
        handleCardinalityIncrement(cardinalityIncrement)
      case count: ObjectCountSignal =>
        handleObjectCount(count)
      case count: SubjectCountSignal =>
        handleSubjectCount(count)
      case other => throw new Exception(s"Unexpected signal @ $id: $other")
    }
    true
  }

}
