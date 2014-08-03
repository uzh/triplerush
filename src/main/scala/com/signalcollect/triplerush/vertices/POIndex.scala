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
import com.signalcollect.triplerush.EfficientIndexPattern
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.SubjectCountSignal
import com.signalcollect.util.SplayIntSet

final class POIndex(id: Long) extends OptimizedIndexVertex(id)
  with Binding {

  @inline def bindIndividualQuery(childDelta: Int, query: Array[Int]): Array[Int] = {
    query.bind(childDelta, id.p, id.o)
  }

  override def afterInitialization(graphEditor: GraphEditor[Long, Any]) {
    super.afterInitialization(graphEditor)
  }

  override def onEdgeAdded(ge: GraphEditor[Long, Any]) {
    incrementParentIndexCardinalities(ge)
    updatePredicateSubjectCount(ge)
  }

  def updatePredicateSubjectCount(ge: GraphEditor[Long, Any]) {
    ge.sendSignal(SubjectCountSignal(edgeCount), EfficientIndexPattern(0, id.p, 0))
  }

}
