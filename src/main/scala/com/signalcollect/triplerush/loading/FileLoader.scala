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

package com.signalcollect.triplerush.loading

import java.io.FileInputStream
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.Dictionary
import com.signalcollect.triplerush.PlaceholderEdge
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.EfficientIndexPattern

case object FileLoader {

  def addEncodedTriple(sId: Int, pId: Int, oId: Int, graphEditor: GraphEditor[Long, Any]) {
    assert(sId > 0 && pId > 0 && oId > 0)
    val po = EfficientIndexPattern(0, pId, oId)
    val so = EfficientIndexPattern(sId, 0, oId)
    val sp = EfficientIndexPattern(sId, pId, 0)
    graphEditor.addEdge(po, new PlaceholderEdge(sId))
    graphEditor.addEdge(so, new PlaceholderEdge(pId))
    graphEditor.addEdge(sp, new PlaceholderEdge(oId))
  }

}
