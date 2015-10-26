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

import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration.{ Duration, DurationInt }

import com.signalcollect.{ ExecutionConfiguration, GraphBuilder }
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.interfaces.MapperFactory
import com.signalcollect.triplerush.dictionary._
import com.signalcollect.triplerush.handlers._
import com.signalcollect.triplerush.loading._
import com.signalcollect.triplerush.mapper._
import com.signalcollect.triplerush.sparql.VariableEncoding
import com.signalcollect.triplerush.util._
import com.signalcollect.triplerush.vertices.RootIndex
import com.signalcollect.triplerush.vertices.blocking.BlockingTripleAdditionsVertex
import com.signalcollect.triplerush.vertices.query._
import com.typesafe.config._

import akka.cluster.Cluster

/**
 * Global accessor for the console visualization.
 */
object TrGlobal {
  var dictionary: Option[RdfDictionary] = None
}

object TripleRush {

  val defaultBlockingOperationTimeout: Duration = 1.hours

  def apply(graphBuilder: GraphBuilder[Long, Any] = new GraphBuilder[Long, Any](),
            dictionary: RdfDictionary = new HashDictionary(),
            tripleMapperFactory: Option[MapperFactory[Long]] = None,
            console: Boolean = false,
            config: Config = ConfigFactory.load().getConfig("signalcollect"),
            kryoRegistrations: List[String] = Kryo.defaultRegistrations): TripleRush = {
    new TripleRush(
      graphBuilder, dictionary, tripleMapperFactory, console, config, kryoRegistrations)
  }
}

/**
 * `fastStart`: Faster startup time, might delay first query execution times and
 * allows to skip calling `prepareExecution`.
 */
class TripleRush(
    val graphBuilder: GraphBuilder[Long, Any],
    val dictionary: RdfDictionary,
    tripleMapperFactory: Option[MapperFactory[Long]],
    console: Boolean,
    config: Config,
    kryoRegistrations: List[String]) extends QueryEngine with BlockingOperations with ConvenienceOperations {
  import TripleRush._

  // Begin initialization ===========
  TrGlobal.dictionary = Some(dictionary)
  private[this] val scConfig = graphBuilder.config
  val numberOfNodes = {
    scConfig.preallocatedNodes.map(_.size).getOrElse(scConfig.nodeProvisioner.numberOfNodes)
  }
  val actorNamePrefix = scConfig.actorNamePrefix
  val graph = graphBuilder.
    withConsole(console).
    withKryoInitializer("com.signalcollect.triplerush.serialization.TripleRushKryoInit").
    withKryoRegistrations(kryoRegistrations).
    withMessageBusFactory(new CombiningMessageBusFactory(8096, 1024)).
    withUndeliverableSignalHandlerFactory(TripleRushUndeliverableSignalHandlerFactory).
    withEdgeAddedToNonExistentVertexHandlerFactory(TripleRushEdgeAddedToNonExistentVertexHandlerFactory).
    withMapperFactory(
      if (numberOfNodes > 1) {
        DistributedTripleMapperFactory
      } else {
        SingleNodeTripleMapperFactory
      }).
      withStorageFactory(TripleRushStorage).
      withThrottlingEnabled(false).
      withThrottlingDuringLoadingEnabled(true).
      withWorkerFactory(new TripleRushWorkerFactory[Any]).
      withBlockingGraphModificationsSupport(false).
      withStatsReportingInterval(500).
      withEagerIdleDetection(false).build
  implicit private[this] val executionContext = graph.system.dispatcher
  private[this] val system = graph.system
  private[this] val cluster = Cluster(system)
  val log = system.log
  private[this] val noOperationsWhenShutdownMessage = "TripleRush has shut down, cannot execute this operation."
  private[this] var _isShutdown = false
  cluster.registerOnMemberRemoved {
    if (!_isShutdown) {
      val msg = "Cluster unexpectedly lost a member, TripleRush can no longer operate and is shutting down."
      println(msg)
      log.info(msg)
      shutdown()
    }
  }
  graph.addVertex(new RootIndex)
  graph.execute(ExecutionConfiguration().withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  // End initialization ============= 

  def isShutdown = _isShutdown

  def asyncAddTriplePatterns(i: Iterator[TriplePattern]): Future[Unit] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    val promise = Promise[Unit]()
    val vertex = new BlockingTripleAdditionsVertex(i, promise)
    graph.addVertex(vertex)
    promise.future
  }

  def asyncAddEncodedTriple(sId: Int, pId: Int, oId: Int): Future[Unit] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    assert(sId > 0 && pId > 0 && oId > 0)
    val promise = Promise[Unit]()
    val vertex = new BlockingTripleAdditionsVertex(Some(TriplePattern(sId, pId, oId)).iterator, promise)
    graph.addVertex(vertex)
    promise.future
  }

  def asyncCount(
    q: Seq[TriplePattern],
    tickets: Long = Long.MaxValue): Future[Option[Long]] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    // Efficient counting query.
    val resultCountPromise = Promise[Option[Long]]()
    val v = new ResultCountingQueryVertex(q, tickets, resultCountPromise)
    graph.addVertex(v)
    resultCountPromise.future
  }

  def asyncGetIndexAt(indexId: Long): Future[Array[Int]] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    val childIdPromise = Promise[Array[Int]]()
    graph.addVertex(new IndexQueryVertex(indexId, childIdPromise))
    childIdPromise.future
  }

  def resultIteratorForQuery(
    query: Seq[TriplePattern],
    numberOfSelectVariables: Option[Int] = None,
    tickets: Long = Long.MaxValue): Iterator[Array[Int]] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    val selectVariables = numberOfSelectVariables.getOrElse(
      VariableEncoding.requiredVariableBindingsSlots(query))
    val resultIterator = new ResultIterator
    val queryVertex = new ResultIteratorQueryVertex(query, selectVariables, tickets, resultIterator)
    graph.addVertex(queryVertex)
    resultIterator
  }

  def shutdown(): Unit = {
    _isShutdown = true
    dictionary.close()
    graph.shutdown()
    val terminationFuture = system.terminate()
    Await.result(terminationFuture, Duration.Inf)
  }

  def edgesPerIndexType: Map[String, Int] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    graph.aggregate(new EdgesPerIndexType)
  }

  def countVerticesByType: Map[String, Int] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    graph.aggregate(new CountVerticesByType)
  }

}
