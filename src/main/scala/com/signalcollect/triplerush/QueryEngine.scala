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

package com.signalcollect.triplerush

import java.io.FileInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect._
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.factory.messagebus.ParallelBulkAkkaMessageBusFactory
import com.signalcollect.triplerush.Expression.int2Expression
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.factory.messagebus.AkkaMessageBusFactory
import java.io.DataInputStream
import java.io.EOFException

case object UndeliverableSignalHandler {
  def handle(signal: Any, targetId: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) {
    signal match {
      case query: PatternQuery =>
        graphEditor.sendSignal(query.tickets, query.queryId, None)
      case other =>
        println(s"Failed signal delivery $other of type ${other.getClass}")
    }
  }
}

case object VertexPreparation {
  def prepareVertex(graphEditor: GraphEditor[Any, Any])(v: Vertex[_, _]) {
    v match {
      case v: IndexVertex =>
        v.optimizeEdgeRepresentation
        v.computeCardinality(graphEditor)
      case v: BindingIndexVertex =>
        v.optimizeEdgeRepresentation
      case other => throw new Exception(s"Only index vertices expected, but found vertex $other")
    }
  }
}

case object FileLoaders {
  def loadNtriplesFile(ntriplesFilename: String)(graphEditor: GraphEditor[Any, Any]) {
    val is = new FileInputStream(ntriplesFilename)
    val nxp = new NxParser(is)
    println(s"Reading triples from $ntriplesFilename ...")
    var triplesLoaded = 0
    while (nxp.hasNext) {
      val triple = nxp.next
      val predicateString = triple(1).toString
      val subjectString = triple(0).toString
      val objectString = triple(2).toString
      val sId = Mapping.register(subjectString)
      val pId = Mapping.register(predicateString)
      val oId = Mapping.register(objectString)
      addTriple(sId, pId, oId, graphEditor)
      triplesLoaded += 1
      if (triplesLoaded % 10000 == 0) {
        println(s"Loaded $triplesLoaded triples from file $ntriplesFilename ...")
      }
    }
    println(s"Done loading triples from $ntriplesFilename. Loaded a total of $triplesLoaded triples.")
    is.close
  }

  def loadBinaryFile(binaryFilename: String)(graphEditor: GraphEditor[Any, Any]) {
    val is = new FileInputStream(binaryFilename)
    val dis = new DataInputStream(is)
    println(s"Reading triples from $binaryFilename ...")
    var triplesLoaded = 0
    try {
      while (true) {
        val sId = dis.readInt
        val pId = dis.readInt
        val oId = dis.readInt
        addTriple(sId, pId, oId, graphEditor)
        triplesLoaded += 1
        if (triplesLoaded % 10000 == 0) {
          println(s"Loaded $triplesLoaded triples from file $binaryFilename ...")
        }
      }
    } catch {
      case done: EOFException =>
        println(s"Done loading triples from $binaryFilename. Loaded a total of $triplesLoaded triples.")
        dis.close
        is.close
      case t: Throwable =>
        throw t
    }
  }

  protected def addTriple(sId: Int, pId: Int, oId: Int, graphEditor: GraphEditor[Any, Any]) {
    val tp = TriplePattern(sId, pId, oId)
    for (parentPattern <- tp.parentPatterns) {
      val idDelta = tp.parentIdDelta(parentPattern)
      graphEditor.addVertex(new BindingIndexVertex(parentPattern))
      graphEditor.addEdge(parentPattern, new PlaceholderEdge(idDelta))
    }
  }
}

case class QueryEngine(graphBuilder: GraphBuilder[Any, Any] = GraphBuilder.withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false))) {
  println("Graph engine is initializing ...")
  private val g = graphBuilder.build
  print("Awaiting idle ... ")
  g.awaitIdle
  println("Done")
  print("Setting undeliverable signal handler ... ")
  g.setUndeliverableSignalHandler(UndeliverableSignalHandler.handle _)
  println("Done")
  print("Starting continuous asynchronous mode ... ")
  g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  println("Done")
  print("Awaiting idle ... ")
  g.awaitIdle
  println("Done")
  println("Graph engine is fully initialized.")

  def loadNtriples(ntriplesFilename: String) {
    g.modifyGraph(FileLoaders.loadNtriplesFile(ntriplesFilename) _, None)
  }

  def loadBinary(binaryFilename: String) {
    g.modifyGraph(FileLoaders.loadBinaryFile(binaryFilename) _, None)
  }

  def executeQuery(q: PatternQuery, optimizer: QueryOptimizer.Value = QueryOptimizer.Greedy): Future[(List[PatternQuery], Map[String, Any])] = {
    require(queryExecutionPrepared)
    val p = promise[(List[PatternQuery], Map[String, Any])]
    if (!q.unmatched.isEmpty) {
      g.addVertex(new QueryVertex(q, p, optimizer))
      p.future
    } else {
      p success (List(), Map().withDefaultValue(""))
      p.future
    }
  }

  private var queryExecutionPrepared = false

  def prepareQueryExecution {
    println("Preparing query execution and awaiting idle.")
    g.awaitIdle
    println("Prepared query execution and preparing vertices.")
    g.foreachVertexWithGraphEditor(VertexPreparation.prepareVertex _)
    println("Done preparing vertices. Awaiting idle again")
    g.awaitIdle
    println("Done awaiting idle")
    //    g.foreachVertex(v => v match {
    //      case v: IndexVertex => println(s"Id: ${v.id}, Card: ${v.cardinality}")
    //      case other => 
    //    })
    //    g.awaitIdle
    queryExecutionPrepared = true
  }

  def awaitIdle {
    g.awaitIdle
  }

  def shutdown = g.shutdown

}