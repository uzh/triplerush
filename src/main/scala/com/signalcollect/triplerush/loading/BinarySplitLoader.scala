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
import com.signalcollect.GraphEditor
import java.io.EOFException
import java.io.DataInputStream
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.PlaceholderEdge

/**
 * Only works if the file contains at least one triple.
 */
case class BinarySplitLoader(binaryFilename: String) extends Iterator[GraphEditor[Any, Any] => Unit] {

  var is: FileInputStream = _
  var dis: DataInputStream = _

  var isInitialized = false

  protected def readNextTriplePattern: TriplePattern = {
    try {
      val sId = dis.readInt
      val pId = dis.readInt
      val oId = dis.readInt
      val tp = TriplePattern(sId, pId, oId)
      tp
    } catch {
      case done: EOFException =>
        dis.close
        is.close
        null.asInstanceOf[TriplePattern]
      case t: Throwable =>
        println(t)
        throw t
    }
  }

  var nextTriplePattern: TriplePattern = null

  def initialize {
    is = new FileInputStream(binaryFilename)
    dis = new DataInputStream(is)
    nextTriplePattern = readNextTriplePattern
    isInitialized = true
  }

  def hasNext = {
    if (!isInitialized) {
      true
    } else {
      nextTriplePattern != null
    }
  }

  def next: GraphEditor[Any, Any] => Unit = {
    if (!isInitialized) {
      initialize
    }
    val loader: GraphEditor[Any, Any] => Unit = addEncodedTriple(
      nextTriplePattern.s, nextTriplePattern.p, nextTriplePattern.o, _)
    nextTriplePattern = readNextTriplePattern
    loader
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int, graphEditor: GraphEditor[Any, Any]) {
    assert(sId > 0 && pId > 0 && oId > 0)
    val po = TriplePattern(0, pId, oId)
    val so = TriplePattern(sId, 0, oId)
    val sp = TriplePattern(sId, pId, 0)
    graphEditor.addEdge(po, new PlaceholderEdge(sId))
    graphEditor.addEdge(so, new PlaceholderEdge(pId))
    graphEditor.addEdge(sp, new PlaceholderEdge(oId))
  }

}
