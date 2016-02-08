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

/**
 * This vertex represents part of the TripleRush index.
 */
abstract class IndexVertex[StateType](val id: Long)
    extends BaseVertex[StateType] {

  override def expose: Map[String, Any] = {
    val dOption = TrGlobal.dictionary
    def resolveWithDictionary(id: Int): String = {
      dOption match {
        case None    => "Dictionary is unavailable."
        case Some(d) => d.get(id).getOrElse("*")
      }
    }
    val indexType = getClass.getSimpleName
    val p = new EfficientIndexPattern(id)
    Map[String, Any](
      "Outgoing edges" -> targetIds.size,
      "Subject" -> resolveWithDictionary(p.s),
      "Predicate" -> resolveWithDictionary(p.p),
      "Object" -> resolveWithDictionary(p.o),
      "TriplePattern" -> s"(sId=${p.s}, pId=${p.p}, oId=${p.o})")
  }

  def foreachChildDelta(f: Int => Unit): Unit

  def addChildDelta(delta: Int): Boolean

  def processQuery(query: Array[Int], graphEditor: GraphEditor[Long, Any]): Unit

  def handleCardinalityIncrement(i: Int): Unit = {}

  def cardinality: Int

  override def addEdge(e: Edge[Long], graphEditor: GraphEditor[Long, Any]): Boolean = {
    e match {
      case b: BlockingIndexVertexEdge =>
        val ticketsToReturn = b.tickets
        val childDelta = b.childDelta
        val operationId = b.blockingOperationId
        val wasAdded = addChildDelta(childDelta)
        val operationVertexId = OperationIds.embedInLong(operationId)
        graphEditor.sendSignal(ticketsToReturn, operationVertexId)
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
      case ChildIdRequest(requestor) =>
        handleChildIdRequest(requestor, graphEditor)
      case cardinalityIncrement: Int =>
        handleCardinalityIncrement(cardinalityIncrement)
      case other: Any => throw new Exception(s"Unexpected signal @ $id: $other")
    }
    true
  }

}
