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
import scala.concurrent.duration.Duration

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
import rx.lang.scala.Observable
import rx.lang.scala.Observer
import rx.lang.scala.Subscription
import rx.lang.scala.subjects.ReplaySubject

case object RegisterQueryResultRecipient

case object UndeliverableRerouter {
  def handle(signal: Any, targetId: Any, sourceId: Option[Any], graphEditor: GraphEditor[Any, Any]) {
    // TODO: Handle root pattern.
    if (targetId == TriplePattern(0, 0, 0)) {
      throw new Exception("Root pattern is not supported.")
    }
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
  //.withLoggingLevel(Logging.DebugLevel)
  console: Boolean = false) extends QueryEngine {

  var canExecute = false

  def prepareExecution {
    g.awaitIdle
    g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    g.awaitIdle
    canExecute = true
  }

  // TODO: Handle root pattern(s).
  // TODO: Validate/simplify queries before executing them.

  println("Graph engine is initializing ...")
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
      "akka.actor.RepointableActorRef")).build
  g.setUndeliverableSignalHandler(UndeliverableRerouter.handle _)
  val system = ActorSystemRegistry.retrieve("SignalCollect").get
  implicit val executionContext = system.dispatcher
  println("TripleRush is ready.")

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

  def executeQuery(q: Array[Int], optimizer: Boolean): Iterable[Array[Int]] = {
    if (optimizer) {
      executeQuery(q, QueryOptimizer.Clever)
    } else {
      executeQuery(q, QueryOptimizer.None)
    }
  }

  def executeQuery(q: Array[Int], optimizer: Int = QueryOptimizer.Clever): Iterable[Array[Int]] = {
    val (results, stats) = executeReactive(q, optimizer)
    results.toBlockingObservable.toIterable
  }

  /**
   * Observable supports only one observer to avoid excessive result caching.
   */
  def executeReactive(
    q: Array[Int],
    optimizer: Int = QueryOptimizer.Clever): (Observable[Array[Int]], Future[Map[Any, Any]]) = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    if (!q.isResult) {
      val resultReporting = new ResultReporting
      val statsPromise = Promise[Map[Any, Any]]()
      g.addVertex(new QueryVertex(q, resultReporting, statsPromise, optimizer))
      implicit val timeout = Timeout(Duration.create(7200, TimeUnit.SECONDS))
      (resultReporting.observable, statsPromise.future)
    } else {
      (Observable(), (Future.successful(Map())))
    }
  }

  def awaitIdle {
    g.awaitIdle
  }

  def shutdown = g.shutdown

}

class ResultReporting {
  var observer: Observer[Array[Int]] = null.asInstanceOf[Observer[Array[Int]]]
  var subscriptionCanceled = false
  var isDone = false
  var unreportedResults: UnrolledBuffer[Array[Array[Int]]] = UnrolledBuffer()

  def reportResult(bindingsArray: Array[Array[Int]]) {
    var shouldReportDirectly = false
    synchronized {
      if (unreportedResults != null && !subscriptionCanceled) {
        unreportedResults.concat(UnrolledBuffer(bindingsArray))
      } else {
        shouldReportDirectly = true
      }
    }
    if (shouldReportDirectly && !subscriptionCanceled) {
      for (bindings <- bindingsArray) {
        observer.onNext(bindings)
      }
    }
  }

  def executionFinished = {
    synchronized {
      isDone = true
      if (observer != null) {
        observer.onCompleted
      }
    }
  }

  def setObserver(o: Observer[Array[Int]]) = {
    synchronized {
      assert(observer == null, "TripleRush supports only one result observer.")
      observer = o
      reportUnreported
      if (isDone) {
        observer.onCompleted
      }
    }
  }

  def reportUnreported = {
    var unreported = null.asInstanceOf[UnrolledBuffer[Array[Array[Int]]]]
    synchronized {
      unreported = unreportedResults
      unreportedResults = null
    }
    if (unreported != null) {
      for (buffer <- unreported) {
        for (bindings <- buffer) {
          observer.onNext(bindings)
        }
      }
    }
  }

  def observable: Observable[Array[Int]] = {
    Observable {
      o: Observer[Array[Int]] =>
        setObserver(o)
        Subscription {
          subscriptionCanceled = true
        }
    }
  }
}
