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

import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.QueryParticle._
import com.signalcollect.util.IntSet
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.SubjectCountSignal

final class POIndex(id: TriplePattern) extends OptimizedIndexVertex(id)
  with Binding {

  assert(id.s == 0 && id.p != 0 && id.o != 0)

  @inline def bindIndividualQuery(childDelta: Int, query: Array[Int]): Array[Int] = {
    query.bind(childDelta, id.p, id.o)
  }

  override def incrementParentIndexCardinalities(ge: GraphEditor[Any, Any]) {
    for (parent <- id.parentPatterns) {
      ge.sendSignal(1, parent, None)
    }
    ge.sendSignal(SubjectCountSignal(edgeCount), TriplePattern(0, id.p, 0), None)
  }
}
