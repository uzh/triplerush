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

import java.util.concurrent.Executors

import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.lang.{ PipedRDFIterator, PipedTriplesStream }

import org.apache.jena.graph.{ Triple => JenaTriple }
import com.signalcollect.GraphEditor
import com.signalcollect.triplerush.{ EfficientIndexPattern, IndexVertexEdge }
import com.signalcollect.triplerush.dictionary.Dictionary
import com.signalcollect.triplerush.sparql.NodeConversion

case class FileLoader(filePath: String, dictionary: Dictionary) extends Iterator[GraphEditor[Long, Any] => Unit] {

  val tripleIterator = new PipedRDFIterator[JenaTriple]
  val inputStream = new PipedTriplesStream(tripleIterator)
  val executor = Executors.newSingleThreadExecutor
  val parser = new Runnable {
    def run: Unit = {
      RDFDataMgr.parse(inputStream, filePath)
    }
  }
  executor.submit(parser)

  def hasNext = {
    tripleIterator.hasNext
  }

  def next: GraphEditor[Long, Any] => Unit = {
    val triple = tripleIterator.next
    val subjectString = NodeConversion.nodeToString(triple.getSubject)
    val predicateString = NodeConversion.nodeToString(triple.getPredicate)
    val objectString = NodeConversion.nodeToString(triple.getObject)
    val sId = dictionary(subjectString)
    val pId = dictionary(predicateString)
    val oId = dictionary(objectString)
    val loader: GraphEditor[Long, Any] => Unit = FileLoader.addEncodedTriple(
      sId, pId, oId, _)
    loader
  }

}

case object FileLoader {

  def addEncodedTriple(sId: Int, pId: Int, oId: Int, graphEditor: GraphEditor[Long, Any]): Unit = {
    assert(sId > 0 && pId > 0 && oId > 0)
    val po = EfficientIndexPattern(0, pId, oId)
    val so = EfficientIndexPattern(sId, 0, oId)
    val sp = EfficientIndexPattern(sId, pId, 0)
    graphEditor.addEdge(po, new IndexVertexEdge(sId))
    graphEditor.addEdge(so, new IndexVertexEdge(pId))
    graphEditor.addEdge(sp, new IndexVertexEdge(oId))
  }

}
