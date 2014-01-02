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

import java.io.DataInputStream
import java.io.EOFException
import java.io.FileInputStream
import java.util.concurrent.TimeUnit
import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.mutable.UnrolledBuffer
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.GraphBuilder
import com.signalcollect.GraphEditor
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.vertices.POIndex
import com.signalcollect.triplerush.vertices.QueryOptimizer
import com.signalcollect.triplerush.vertices.QueryVertex
import com.signalcollect.triplerush.vertices.SOIndex
import com.signalcollect.triplerush.vertices.SPIndex
import akka.util.Timeout
import scala.concurrent.Await
import com.signalcollect.interfaces.AggregationOperation
import com.signalcollect.Vertex
import com.signalcollect.interfaces.ModularAggregationOperation
import com.signalcollect.triplerush.vertices.RootIndex

case object RegisterQueryResultRecipient

case object UndeliverableRerouter {
  def handle(signal: Any, targetId: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) {
    signal match {
      case queryParticle: Array[Int] =>
        graphEditor.sendSignal(queryParticle.tickets, queryParticle.queryId, None)
      case CardinalityRequest(forPattern: TriplePattern, requestor: AnyRef) =>
        graphEditor.sendSignal(CardinalityReply(forPattern, 0), requestor, None)
      case CardinalityReply(forPattern, cardinality) =>
      // Do nothing, query vertex has removed itself already because of a 0 cardinality pattern.
      case other =>
        println(s"Failed signal delivery of $other of type ${other.getClass} to the vertex with id $targetId and sender id $sourceId.")
    }
  }
}

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
    val patternCopy = nextTriplePattern
    val loader: GraphEditor[Any, Any] => Unit = FileLoaders.addTriple(patternCopy, _)
    nextTriplePattern = readNextTriplePattern
    loader
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
      val tp = TriplePattern(sId, pId, oId)
      if (!tp.isFullyBound) {
        println(s"Problem: $tp, triple #${triplesLoaded + 1} in file $ntriplesFilename is not fully bound.")
      } else {
        addTriple(tp, graphEditor)
      }
      triplesLoaded += 1
      if (triplesLoaded % 10000 == 0) {
        println(s"Loaded $triplesLoaded triples from file $ntriplesFilename ...")
      }
    }
    println(s"Done loading triples from $ntriplesFilename. Loaded a total of $triplesLoaded triples.")
    is.close
  }

  def addTriple(tp: TriplePattern, graphEditor: GraphEditor[Any, Any]) {
    assert(tp.isFullyBound)
    val po = TriplePattern(0, tp.p, tp.o)
    val so = TriplePattern(tp.s, 0, tp.o)
    val sp = TriplePattern(tp.s, tp.p, 0)
    graphEditor.addVertex(new POIndex(po))
    graphEditor.addVertex(new SOIndex(so))
    graphEditor.addVertex(new SPIndex(sp))
    graphEditor.addEdge(po, new PlaceholderEdge(tp.s))
    graphEditor.addEdge(so, new PlaceholderEdge(tp.p))
    graphEditor.addEdge(sp, new PlaceholderEdge(tp.o))
  }
}

case class TripleRush(
  graphBuilder: GraphBuilder[Any, Any] = GraphBuilder,
  // TODO: Ensure placement of Query vertices on coordinator node.
  //.withLoggingLevel(Logging.DebugLevel)
  console: Boolean = false) extends QueryEngine {

  // TODO: Handle root pattern(s).
  // TODO: Validate/simplify queries before executing them.

  println("Graph engine is initializing ...")

  var canExecute = false

  private val g = graphBuilder.withConsole(console).
    withMessageBusFactory(new CombiningMessageBusFactory(8096, false)).
    withMapperFactory(TripleMapperFactory).
    //    withMessageSerialization(true).
    //    withJavaSerialization(false).
    withHeartbeatInterval(500).
    withKryoRegistrations(List(
      "com.signalcollect.triplerush.vertices.SIndex",
      "com.signalcollect.triplerush.vertices.PIndex",
      "com.signalcollect.triplerush.vertices.OIndex",
      "com.signalcollect.triplerush.vertices.SPIndex",
      "com.signalcollect.triplerush.vertices.POIndex",
      "com.signalcollect.triplerush.vertices.SOIndex",
      "com.signalcollect.triplerush.TriplePattern",
      "com.signalcollect.triplerush.vertices.QueryVertex",
      "com.signalcollect.triplerush.vertices.QueryOptimizer",
      "com.signalcollect.triplerush.PlaceholderEdge",
      "com.signalcollect.triplerush.CardinalityRequest",
      "com.signalcollect.triplerush.CardinalityReply",
      "com.signalcollect.triplerush.TriplePattern",
      "Array[com.signalcollect.triplerush.TriplePattern]",
      "com.signalcollect.interfaces.SignalMessage$mcIJ$sp",
      "com.signalcollect.interfaces.AddEdge",
      "akka.actor.RepointableActorRef")).build
  g.setUndeliverableSignalHandler(UndeliverableRerouter.handle _)
  val system = ActorSystemRegistry.retrieve("SignalCollect").get
  implicit val executionContext = system.dispatcher
  g.addVertex(new RootIndex)
  println("TripleRush is ready.")

  def prepareExecution {
    g.awaitIdle
    g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    g.awaitIdle
    canExecute = true
  }

  def loadNtriples(ntriplesFilename: String, placementHint: Option[Any] = None) {
    g.modifyGraph(FileLoaders.loadNtriplesFile(ntriplesFilename) _, placementHint)
  }

  def loadBinary(binaryFilename: String, placementHint: Option[Any] = None) {
    g.loadGraph(BinarySplitLoader(binaryFilename), placementHint)
  }

  /**
   * Slow, only use for debugging purposes.
   */
  def addTriple(s: String, p: String, o: String) {
    val sId = Mapping.register(s)
    val pId = Mapping.register(p)
    val oId = Mapping.register(o)
    val tp = TriplePattern(sId, pId, oId)
    FileLoaders.addTriple(tp, g)
  }

  /**
   * Slow, only use for debugging purposes.
   */
  def addEncodedTriple(sId: Int, pId: Int, oId: Int) {
    FileLoaders.addTriple(TriplePattern(sId, pId, oId), g)
  }

  def executeQuery(q: Array[Int]): Traversable[Array[Int]] = {
    val (resultFuture, statsFuture) = executeAdvancedQuery(q, QueryOptimizer.Clever)
    val result = Await.result(resultFuture, 7200.seconds)
    result
  }

  def executeAdvancedQuery(
    q: Array[Int],
    optimizer: Int = QueryOptimizer.Clever): (Future[Traversable[Array[Int]]], Future[Map[Any, Any]]) = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val resultPromise = Promise[Traversable[Array[Int]]]()
    val statsPromise = Promise[Map[Any, Any]]()
    g.addVertex(new QueryVertex(q, resultPromise, statsPromise, optimizer))
    if (!q.isResult) {
      // Only check if result once computation is running.
      (resultPromise.future, statsPromise.future)
    } else {
      (Future.successful(List()), (Future.successful(Map())))
    }
  }

  def awaitIdle {
    g.awaitIdle
  }

  def shutdown = {
    g.shutdown
  }

  def edgesPerIndexType: Map[String, Int] = {
    g.aggregate(new EdgesPerIndexType)
  }

  def countVerticesByType: Map[String, Int] = {
    g.aggregate(new CountVerticesByType)
  }

}

case class EdgesPerIndexType() extends AggregationOperation[Map[String, Int]] {
  def extract(v: Vertex[_, _]): Map[String, Int] = {
    Map(v.getClass.toString -> v.edgeCount).withDefaultValue(0)
  }
  def reduce(elements: Stream[Map[String, Int]]): Map[String, Int] = {
    val result: Map[String, Int] = elements.reduce { (m1: Map[String, Int], m2: Map[String, Int]) =>
      val keys = m1.keys ++ m2.keys
      val merged = keys.map(k => (k, m1(k) + m2(k)))
      merged.toMap.withDefaultValue(0)
    }
    result
  }
}

case class CountVerticesByType() extends AggregationOperation[Map[String, Int]] {
  def extract(v: Vertex[_, _]): Map[String, Int] = {
    Map(v.getClass.toString -> 1).withDefaultValue(0)
  }
  def reduce(elements: Stream[Map[String, Int]]): Map[String, Int] = {
    val result: Map[String, Int] = elements.reduce { (m1: Map[String, Int], m2: Map[String, Int]) =>
      val keys = m1.keys ++ m2.keys
      val merged = keys.map(k => (k, m1(k) + m2(k)))
      merged.toMap.withDefaultValue(0)
    }
    result
  }
}
