/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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
import com.signalcollect.triplerush.TriplePattern

case class NtriplesLoader(ntriplesFilename: String, dictionary: Dictionary) extends Iterator[GraphEditor[Long, Any] => Unit] {

  var is: FileInputStream = _
  var nxp: NxParser = _

  var isInitialized = false
  var nextTriplePattern: TriplePattern = null

  def initialize {
    is = new FileInputStream(ntriplesFilename)
    nxp = new NxParser(is)
    isInitialized = true
  }

  def hasNext = {
    if (!isInitialized) {
      initialize
    }
    val hasNext = nxp.hasNext
    if (!hasNext) {
      is.close
    }
    hasNext
  }

  def next: GraphEditor[Long, Any] => Unit = {
    if (!isInitialized) {
      initialize
    }
    val triple = nxp.next
    val predicateString = triple(1).toString
    val subjectString = triple(0).toString
    val objectString = triple(2).toString
    val sId = dictionary(subjectString)
    val pId = dictionary(predicateString)
    val oId = dictionary(objectString)
    val loader: GraphEditor[Long, Any] => Unit = FileLoader.addEncodedTriple(
      sId, pId, oId, _)
    loader
  }

}
