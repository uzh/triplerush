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
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.promise
import scala.util.Random
import org.semanticweb.yars.nx.parser.NxParser
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.GraphBuilder
import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.ExecutionMode
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.pattern.ask
import akka.util.Timeout
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

case object UndeliverableSignalHandler {
  def handle(signal: Any, targetId: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) {
    signal match {
      case query: PatternQuery =>
        graphEditor.sendSignal(query.tickets, query.queryId, None)
      case CardinalityRequest(forPattern: TriplePattern, requestor: AnyRef) =>
        graphEditor.sendSignal(CardinalityReply(forPattern, 0), requestor, None)
      case other =>
        println(s"Failed signal delivery of $other of type ${other.getClass} to the vertex with id $targetId and sender id $sourceId.")
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

case object RegisterQueryResultRecipient

class ResultRecipientActor extends Actor {
  var result: Any = _
  var queryResultRecipient: ActorRef = _

  def receive = {
    case RegisterQueryResultRecipient =>
      queryResultRecipient = sender
      if (result != null) {
        queryResultRecipient ! result
        self ! PoisonPill
      }
    case result: QueryResult =>
      this.result = result
      if (queryResultRecipient != null) {
        queryResultRecipient ! result
        self ! PoisonPill
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
      TriplePattern(sId, pId, oId)
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
      addTriple(TriplePattern(sId, pId, oId), graphEditor)
      triplesLoaded += 1
      if (triplesLoaded % 10000 == 0) {
        println(s"Loaded $triplesLoaded triples from file $ntriplesFilename ...")
      }
    }
    println(s"Done loading triples from $ntriplesFilename. Loaded a total of $triplesLoaded triples.")
    is.close
  }

  def addTriple(tp: TriplePattern, graphEditor: GraphEditor[Any, Any]) {
    for (parentPattern <- tp.parentPatterns) {
      val idDelta = tp.parentIdDelta(parentPattern)
      graphEditor.addVertex(new BindingIndexVertex(parentPattern))
      graphEditor.addEdge(parentPattern, new PlaceholderEdge(idDelta))
    }
  }
}

case class QueryEngine(graphBuilder: GraphBuilder[Any, Any] = GraphBuilder.withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false))) {
  println("Graph engine is initializing ...")
  private val g = graphBuilder.withKryoRegistrations(List(
    "com.signalcollect.triplerush.PatternQuery",
    "com.signalcollect.triplerush.TriplePattern",
    "com.signalcollect.triplerush.BindingIndexVertex",
    "com.signalcollect.triplerush.IndexVertex",
    "com.signalcollect.triplerush.PlaceholderEdge",
    "com.signalcollect.triplerush.CardinalityRequest",
    "com.signalcollect.triplerush.CardinalityReply",
    "com.signalcollect.triplerush.QueryVertex",
    "com.signalcollect.triplerush.QueryOptimizer",
    "com.signalcollect.triplerush.QueryResult",
    "akka.actor.RepointableActorRef")).build
  print("Awaiting idle ... ")
  g.awaitIdle
  println("Done")
  print("Setting undeliverable signal handler ... ")
  g.setUndeliverableSignalHandler(UndeliverableSignalHandler.handle _)
  println("Done")
  val system = ActorSystemRegistry.retrieve("SignalCollect").get
  implicit val executionContext = system.dispatcher
  print("Awaiting idle ... ")
  g.awaitIdle
  println("Done")
  println("Graph engine is fully initialized.")

  def loadNtriples(ntriplesFilename: String, placementHint: Option[Any] = None) {
    g.modifyGraph(FileLoaders.loadNtriplesFile(ntriplesFilename) _, placementHint)
  }

  def loadBinary(binaryFilename: String, placementHint: Option[Any] = None) {
    g.loadGraph(BinarySplitLoader(binaryFilename), placementHint)
  }

  /**
   * Slow, only use for debugging purposes.
   */
  def loadTriple(s: String, p: String, o: String) {
    val sId = Mapping.register(s)
    val pId = Mapping.register(p)
    val oId = Mapping.register(o)
    val tp = TriplePattern(sId, pId, oId)
    for (parentPattern <- tp.parentPatterns) {
      val idDelta = tp.parentIdDelta(parentPattern)
      g.addVertex(new BindingIndexVertex(parentPattern))
      g.addEdge(parentPattern, new PlaceholderEdge(idDelta))
    }
  }

  def executeQuery(q: PatternQuery, optimizer: Int = QueryOptimizer.Clever): Future[QueryResult] = {
    assert(queryExecutionPrepared)
    if (!q.unmatched.isEmpty) {
      val resultRecipientActor = system.actorOf(Props[ResultRecipientActor], name = Random.nextLong.toString)
      g.addVertex(new QueryVertex(q, resultRecipientActor, optimizer))
      implicit val timeout = Timeout(Duration.create(7200, TimeUnit.SECONDS))
      val resultFuture = resultRecipientActor ? RegisterQueryResultRecipient
      resultFuture.asInstanceOf[Future[QueryResult]]
    } else {
      val p = promise[QueryResult]
      p success (QueryResult(Array(), Array()))
      p.future
    }
  }

  private var queryExecutionPrepared = false

  def prepareQueryExecution {
    print("Waiting for graph loading to finish ... ")
    g.awaitIdle
    println("Done")
    print("Starting continuous asynchronous mode ... ")
    g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    println("Done")
    println("Preparing query execution and awaiting idle.")
    g.awaitIdle
    println("Prepared query execution and preparing vertices.")
    g.foreachVertexWithGraphEditor(VertexPreparation.prepareVertex _)
    println("Done preparing vertices. Awaiting idle again")
    g.awaitIdle
    println("Done awaiting idle")
    queryExecutionPrepared = true
  }

  def awaitIdle {
    g.awaitIdle
  }

  def shutdown = g.shutdown

}