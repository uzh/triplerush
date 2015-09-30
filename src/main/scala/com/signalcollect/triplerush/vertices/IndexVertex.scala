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

import com.signalcollect.{ Edge, GraphEditor }
import com.signalcollect.triplerush._
import com.signalcollect.triplerush.EfficientIndexPattern._

/**
 * This vertex represents part of the TripleRush index.
 */
abstract class IndexVertex[StateType](val id: Long)
    extends BaseVertex[StateType] {

  override def expose: Map[String, Any] = {
    val indexType = getClass.getSimpleName
    val d = TrGlobal.dictionary
    val p = new EfficientIndexPattern(id)
    Map[String, Any](
      "Outgoing edges" -> targetIds.size,
      "Subject" -> d.get(p.s),
      "Predicate" -> d.get(p.p),
      "Object" -> d.get(p.o),
      "TriplePattern" -> s"(sId=${p.s}, pId=${p.p}, oId=${p.o})")
  }

  def foreachChildDelta(f: Int => Unit): Unit

  def addChildDelta(delta: Int): Boolean

  def processQuery(query: Array[Int], graphEditor: GraphEditor[Long, Any]): Unit

  def handleCardinalityIncrement(i: Int) = {}

  def handleObjectCount(count: ObjectCountSignal) = {}

  def handleSubjectCount(count: SubjectCountSignal) = {}

  def cardinality: Int

  /**
   * Default reply, is only overridden by SOIndex.
   */
  def handleCardinalityRequest(c: CardinalityRequest, graphEditor: GraphEditor[Long, Any]): Unit = {
    graphEditor.sendSignal(CardinalityReply(
      c.forPattern, cardinality), c.requestor)
  }

  override def addEdge(e: Edge[Long], graphEditor: GraphEditor[Long, Any]): Boolean = {
    e match {
      case b: BlockingIndexVertexEdge =>
        val ticketsToReturn = b.tickets
        //println(s"${id.toTriplePattern} is returning $ticketsToReturn tickets")
        val wasAdded = addChildDelta(b.childDelta)
        val operationVertexId = OperationIds.embedInLong(b.blockingOperationId)
        graphEditor.sendSignal(ticketsToReturn, operationVertexId)
        wasAdded
      case i: IndexVertexEdge =>
        val wasAdded = addChildDelta(i.childDelta)
        wasAdded
      case other: Any =>
        val msg = s"IndexVertex does not support adding edge $e of type ${e.getClass.getSimpleName}."
        val error = new UnsupportedOperationException(msg)
        graphEditor.log.error(error, msg)
        throw error
    }
  }

  def handleChildIdRequest(
    requestor: Long, graphEditor: GraphEditor[Long, Any]): Unit

  override def deliverSignalWithoutSourceId(
    signal: Any,
    graphEditor: GraphEditor[Long, Any]): Boolean = {
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
      case other: Any => throw new Exception(s"Unexpected signal @ $id: $other")
    }
    true
  }

}
