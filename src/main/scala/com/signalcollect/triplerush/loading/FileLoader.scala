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

case object FileLoader {
  def loadNtriplesFile(dictionary: Dictionary, ntriplesFilename: String)(graphEditor: GraphEditor[Any, Any]) {
    val is = new FileInputStream(ntriplesFilename)
    val nxp = new NxParser(is)
    println(s"Reading triples from $ntriplesFilename ...")
    var triplesLoaded = 0
    while (nxp.hasNext) {
      val triple = nxp.next
      val predicateString = triple(1).toString
      val subjectString = triple(0).toString
      val objectString = triple(2).toString
      val sId = dictionary(subjectString)
      val pId = dictionary(predicateString)
      val oId = dictionary(objectString)
      val tp = TriplePattern(sId, pId, oId)
      if (!tp.isFullyBound) {
        println(s"Problem: $tp, triple #${triplesLoaded + 1} in file $ntriplesFilename is not fully bound.")
      } else {
        addEncodedTriple(sId, pId, oId, graphEditor)
      }
      triplesLoaded += 1
      if (triplesLoaded % 10000 == 0) {
        println(s"Loaded $triplesLoaded triples from file $ntriplesFilename ...")
      }
    }
    println(s"Done loading triples from $ntriplesFilename. Loaded a total of $triplesLoaded triples.")
    is.close
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int, graphEditor: GraphEditor[Any, Any]) {
    assert(sId > 0 && pId > 0 && oId > 0)
    val po = TriplePattern(0, pId, oId).toEfficientIndexPattern
    val so = TriplePattern(sId, 0, oId).toEfficientIndexPattern
    val sp = TriplePattern(sId, pId, 0).toEfficientIndexPattern
    graphEditor.addEdge(po, new PlaceholderEdge(sId))
    graphEditor.addEdge(so, new PlaceholderEdge(pId))
    graphEditor.addEdge(sp, new PlaceholderEdge(oId))
  }

}
