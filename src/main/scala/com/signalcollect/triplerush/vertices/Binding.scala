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
import com.signalcollect.DefaultEdge
import com.signalcollect.triplerush.QueryParticle._
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.interfaces.Inspectable
import com.signalcollect.Edge

trait Binding
  extends IndexVertex
  with ParentBuilding[Any, Any] {

  def incrementParentIndexCardinalities(ge: GraphEditor[Any, Any]) {
    for (parent <- id.parentPatterns) {
      ge.sendSignal(1, parent, None)
    }
  }

  def decrementParentIndexCardinalities(ge: GraphEditor[Any, Any]) {
    for (parent <- id.parentPatterns) {
      ge.sendSignal(-1, parent, None)
    }
  }

  override def addEdge(e: Edge[_], ge: GraphEditor[Any, Any]): Boolean = {
    val wasAdded = super.addEdge(e, ge)
    if (wasAdded) {
      incrementParentIndexCardinalities(ge)
    }
    wasAdded
  }

  def bindIndividualQuery(childDelta: Int, queryParticle: Array[Int]): Array[Int]

  def processQuery(query: Array[Int], graphEditor: GraphEditor[Any, Any]) {
    bindQueryToAllTriples(query, graphEditor)
  }

  def bindQueryToAllTriples(query: Array[Int], graphEditor: GraphEditor[Any, Any]) {
    if (!query.isBindingQuery &&
      query.numberOfPatterns == 1 &&
      query.isSimpleToBind) {
      // Take a shortcut and don't actually do the binding, just send the result count.
      // The isSimpleToBind check excludes complicated cases, where a binding might fail.
      graphEditor.sendSignal(edgeCount, query.queryId, None)
      graphEditor.sendSignal(query.tickets, query.queryId, None)
    } else {
      val edges = edgeCount
      val totalTickets = query.tickets
      val avg = math.abs(totalTickets) / edges
      val complete = avg > 0 && totalTickets > 0
      var extras = math.abs(totalTickets) % edges
      val averageTicketQuery = query.copyWithTickets(avg, complete)
      val aboveAverageTicketQuery = query.copyWithTickets(avg + 1, complete)
      def bind(childDelta: Int) {
        if (extras > 0) {
          extras -= 1
          handleQueryBinding(childDelta, aboveAverageTicketQuery, graphEditor)
        } else if (avg > 0) {
          handleQueryBinding(childDelta, averageTicketQuery, graphEditor)
        }
      }
      foreachChildDelta(bind)
    }
  }

  def handleQueryBinding(
    childDelta: Int,
    query: Array[Int],
    graphEditor: GraphEditor[Any, Any]) {
    val boundParticle = bindIndividualQuery(childDelta, query)
    if (boundParticle != null) {
      routeSuccessfullyBound(boundParticle, graphEditor)
    } else {
      // Failed to bind, send to query vertex.
      graphEditor.sendSignal(query.tickets, query.queryId, None)
    }
  }

  def routeSuccessfullyBound(
    boundParticle: Array[Int],
    graphEditor: GraphEditor[Any, Any]) {

    if (boundParticle.isResult) {
      // Query successful, send to query vertex.
      if (boundParticle.isBindingQuery) {
        graphEditor.sendSignal(boundParticle, boundParticle.queryId, None)
      } else {
        graphEditor.sendSignal(1, boundParticle.queryId, None)
        graphEditor.sendSignal(boundParticle.tickets, boundParticle.queryId, None)
      }
    } else {
      graphEditor.sendSignal(boundParticle, boundParticle.routingAddress, None)
    }
  }

}