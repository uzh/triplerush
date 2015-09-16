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

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.jena.riot.Lang
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration.DurationInt
import scala.reflect.ManifestFactory
import scala.reflect.runtime.universe
import com.signalcollect.{ ExecutionConfiguration, GraphBuilder }
import com.signalcollect.configuration.{ ActorSystemRegistry, ExecutionMode }
import com.signalcollect.factory.scheduler.Throughput
import com.signalcollect.interfaces.AddEdge
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.interfaces._
import com.signalcollect.triplerush.dictionary._
import com.signalcollect.triplerush.handlers._
import com.signalcollect.triplerush.loading._
import com.signalcollect.triplerush.mapper._
import com.signalcollect.triplerush.sparql._
import com.signalcollect.triplerush.util._
import com.signalcollect.triplerush.vertices._
import com.signalcollect.triplerush.vertices.query._
import com.signalcollect.examples.PlaceholderEdge
import org.apache.jena.graph.{ Triple => JenaTriple }

/**
 * Global accessor for the console visualization.
 */
object TrGlobal {
  var dictionary: Option[RdfDictionary] = None
}

object TripleRush {
  def apply(graphBuilder: GraphBuilder[Long, Any] = new GraphBuilder[Long, Any](),
            dictionary: RdfDictionary = new HashDictionary(),
            tripleMapperFactory: Option[MapperFactory[Long]] = None,
            fastStart: Boolean = false,
            console: Boolean = false,
            kryoRegistrations: List[String] = Kryo.defaultRegistrations): TripleRush = {
    new TripleRush(graphBuilder, dictionary, tripleMapperFactory, fastStart, console, kryoRegistrations)
  }
}

/**
 * `fastStart`: Faster startup time, might delay first query execution times and
 *  allows to skip calling `prepareExecution`.
 */
class TripleRush(
    graphBuilder: GraphBuilder[Long, Any],
    val dictionary: RdfDictionary,
    tripleMapperFactory: Option[MapperFactory[Long]],
    fastStart: Boolean,
    console: Boolean,
    kryoRegistrations: List[String] = Kryo.defaultRegistrations) extends QueryEngine {

  TrGlobal.dictionary = Some(dictionary)

  val graph = graphBuilder.withConsole(console).
    withMessageBusFactory(new CombiningMessageBusFactory(8096, 1024)).
    withUndeliverableSignalHandlerFactory(TripleRushUndeliverableSignalHandlerFactory).
    withEdgeAddedToNonExistentVertexHandlerFactory(TripleRushEdgeAddedToNonExistentVertexHandlerFactory).
    withKryoInitializer("com.signalcollect.triplerush.serialization.TripleRushKryoInit").
    withMapperFactory(
      tripleMapperFactory.getOrElse(
        if (graphBuilder.config.preallocatedNodes.isEmpty && graphBuilder.config.nodeProvisioner.isInstanceOf[LocalNodeProvisioner[_, _]]) {
          SingleNodeTripleMapperFactory
        } else {
          DistributedTripleMapperFactory
        })).
      withStorageFactory(TripleRushStorage).
      withThrottlingEnabled(false).
      withThrottlingDuringLoadingEnabled(true).
      withWorkerFactory(new TripleRushWorkerFactory[Any]).
      withBlockingGraphModificationsSupport(false).
      withStatsReportingInterval(500).
      withEagerIdleDetection(false).
      withKryoRegistrations(kryoRegistrations).build
  val system = graphBuilder.config.actorSystem.getOrElse(ActorSystemRegistry.retrieve("SignalCollect").get)
  implicit val executionContext = system.dispatcher
  graph.addVertex(new RootIndex)

  private[this] var canExecute = false

  if (fastStart) {
    graph.execute(ExecutionConfiguration().withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    canExecute = true
  }

  def prepareExecution(): Unit = {
    graph.awaitIdle
    graph.execute(ExecutionConfiguration().withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    graph.awaitIdle
    canExecute = true
  }

  /**
   * The placement hint should ensure that this gets processed on node 0, because the dictionary resides on that node.
   * If you get a serialization error for the dictionary, it is probably due to a problematic placement hint.
   */
  def loadFromFile(
    filePath: String,
    placementHint: Option[Long] = Some(OperationIds.embedInLong(OperationIds.nextId))): Unit = {
    val iterator = TripleIterator(filePath)
    loadFromIterator(iterator, placementHint)
  }

  def loadFromStream(
    inputStream: InputStream,
    placementHint: Option[Long] = Some(OperationIds.embedInLong(OperationIds.nextId)), lang: Lang): Unit = {
    val iterator = TripleIterator(inputStream, lang)
    loadFromIterator(iterator, placementHint)
  }

  def loadFromIterator(
    iterator: Iterator[JenaTriple],
    placementHint: Option[Long] = Some(OperationIds.embedInLong(OperationIds.nextId))): Unit = {
    val loader = new DataLoader(iterator, dictionary)
    graph.loadGraph(loader, placementHint)
  }

  /**
   * String encoding:
   * By default something is interpreted as an IRI.
   * If something starts with a hyphen or a digit, it is interpreted as an integer literal
   * If something starts with '"' it is interpreted as a string literal.
   * If something has an extra '<' prefix, then the remainder is interpreted as an XML literal.
   * If something starts with '_', then the remainder is assumed to be a blank node ID where uniqueness is the
   * responsibility of the caller.
   */
  def addStringTriple(s: String, p: String, o: String, blocking: Boolean = false): Unit = {
    val sId = dictionary(s)
    val pId = dictionary(p)
    val oId = dictionary(o)
    addEncodedTriple(sId, pId, oId, blocking)
  }

  def addTriple(triple: JenaTriple, blocking: Boolean = false): Unit = {
    addTriplePattern(DataLoader.toTriplePattern(triple, dictionary), blocking)
  }

  def addTriples(i: Iterator[JenaTriple], blocking: Boolean = false): Unit = {
    val mappedIterator = i.map(DataLoader.toTriplePattern(_, dictionary))
    if (blocking) {
      addTriplePatterns(mappedIterator, blocking)
    } else {
      addTriplePatterns(mappedIterator)
    }
  }

  def addTriplePattern(tp: TriplePattern, blocking: Boolean = false): Unit = {
    addEncodedTriple(tp.s, tp.p, tp.o, blocking)
  }

  def addTriplePatterns(i: Iterator[TriplePattern], blocking: Boolean = false): Unit = {
    if (blocking) {
      assert(canExecute, "Blocking additions only work with `fastStart` or after having called `prepareExecution`.")
      val promise = Promise[Unit]()
      val vertex = new BlockingTripleAdditionsVertex(i, promise)
      graph.addVertex(vertex)
      Await.result(promise.future, 7200.seconds)
    } else {
      Future {
        while (i.hasNext) {
          addTriplePattern(i.next, blocking = false)
        }
      }
    }
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int, blocking: Boolean = false): Unit = {
    assert(sId > 0 && pId > 0 && oId > 0)
    if (blocking) {
      assert(canExecute, "Blocking additions only work with `fastStart` or after having called `prepareExecution`.")
      val promise = Promise[Unit]()
      val vertex = new BlockingTripleAdditionsVertex(Some(TriplePattern(sId, pId, oId)).iterator, promise)
      graph.addVertex(vertex)
      Await.result(promise.future, 7200.seconds)
    } else {
      DataLoader.addEncodedTriple(sId, pId, oId, graph)
    }
  }

  def executeCountingQuery(
    q: Seq[TriplePattern],
    tickets: Long = Long.MaxValue): Future[Option[Long]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    // Efficient counting query.
    val resultCountPromise = Promise[Option[Long]]()
    graph.addVertex(new ResultCountingQueryVertex(q, tickets, resultCountPromise))
    resultCountPromise.future
  }

  /**
   * Blocking version of 'executeIndexQuery'.
   */
  def childIdsForPattern(indexId: Long): Array[Int] = {
    val intArrayFuture = executeIndexQuery(indexId)
    Await.result(intArrayFuture, 7200.seconds)
  }

  def executeIndexQuery(indexId: Long): Future[Array[Int]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val childIdPromise = Promise[Array[Int]]()
    graph.addVertex(new IndexQueryVertex(indexId, childIdPromise))
    childIdPromise.future
  }

  // Delegates, just to implement the interface.
  def resultIteratorForQuery(query: Seq[TriplePattern]): Iterator[Array[Int]] = {
    resultIteratorForQuery(query, None, Long.MaxValue)
  }

  def resultIteratorForQuery(
    query: Seq[TriplePattern],
    numberOfSelectVariables: Option[Int] = None,
    tickets: Long = Long.MaxValue): Iterator[Array[Int]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val selectVariables = numberOfSelectVariables.getOrElse(
      VariableEncoding.requiredVariableBindingsSlots(query))
    val resultIterator = new ResultIterator
    val queryVertex = new ResultIteratorQueryVertex(query, selectVariables, tickets, resultIterator)
    graph.addVertex(queryVertex)
    resultIterator
  }

  def awaitIdle(): Unit = {
    graph.awaitIdle
  }

  def clear(): Unit = {
    clearDictionary
    graph.reset
    graph.awaitIdle
    graph.addVertex(new RootIndex)
  }

  def clearDictionary(): Unit = {
    dictionary.clear
  }

  def shutdown(): Unit = {
    dictionary.close()
    graph.shutdown
  }

  def edgesPerIndexType: Map[String, Int] = {
    graph.aggregate(new EdgesPerIndexType)
  }

  def countVerticesByType: Map[String, Int] = {
    graph.aggregate(new CountVerticesByType)
  }

}

object Kryo {

  val defaultRegistrations: List[String] = {
    List(
      classOf[RootIndex].getName,
      classOf[SIndex].getName,
      classOf[PIndex].getName,
      classOf[OIndex].getName,
      classOf[SPIndex].getName,
      classOf[POIndex].getName,
      classOf[SOIndex].getName,
      classOf[TriplePattern].getName,
      classOf[IndexVertexEdge].getName,
      classOf[BlockingIndexVertexEdge].getName,
      classOf[CardinalityRequest].getName,
      classOf[CardinalityReply].getName,
      classOf[PredicateStatsReply].getName,
      classOf[ChildIdRequest].getName,
      classOf[ChildIdReply].getName,
      classOf[SubjectCountSignal].getName,
      classOf[ObjectCountSignal].getName,
      classOf[TriplePattern].getName,
      classOf[PredicateStats].getName,
      classOf[ResultIteratorQueryVertex].getName,
      classOf[ResultIterator].getName,
      classOf[AtomicBoolean].getName,
      classOf[LinkedBlockingQueue[Any]].getName,
      classOf[TripleRushWorkerFactory[Any]].getName,
      TripleRushEdgeAddedToNonExistentVertexHandlerFactory.getClass.getName,
      TripleRushUndeliverableSignalHandlerFactory.getClass.getName,
      TripleRushStorage.getClass.getName,
      SingleNodeTripleMapperFactory.getClass.getName,
      new AlternativeTripleMapperFactory(false).getClass.getName,
      DistributedTripleMapperFactory.getClass.getName,
      RelievedNodeZeroTripleMapperFactory.getClass.getName,
      LoadBalancingTripleMapperFactory.getClass.getName,
      ManifestFactory.Long.getClass.getName,
      classOf[CombiningMessageBusFactory[_]].getName,
      classOf[AddEdge[Any, Any]].getName,
      classOf[AddEdge[Long, Long]].getName, // TODO: Can we force the use of the specialized version?
      new Throughput[Long, Any].getClass.getName,
      SignalMessageWithoutSourceId[Long, Any](
        signal = null.asInstanceOf[Any],
        targetId = null.asInstanceOf[Long]).getClass.getName,
      BulkSignalNoSourceIds[Long, Any](
        signals = null.asInstanceOf[Array[Any]],
        targetIds = null.asInstanceOf[Array[Long]]).getClass.getName,
      "akka.actor.RepointableActorRef")
  }

}
