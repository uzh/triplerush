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
import com.signalcollect.triplerush.vertices.blocking.TripleAdditionSynchronizationVertex
import com.signalcollect.triplerush.vertices.query._
import com.typesafe.config._
import akka.cluster.Cluster
import scala.util.Success
import scala.util.Failure

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
            indexStructure: IndexStructure = FullIndex,
            console: Boolean = false,
            kryoRegistrations: List[String] = Kryo.defaultRegistrations): TripleRush = {
    new TripleRush(
      graphBuilder, dictionary, tripleMapperFactory, indexStructure, console, kryoRegistrations)
  }
}

class TripleRush(
    val graphBuilder: GraphBuilder[Long, Any],
    val dictionary: RdfDictionary,
    tripleMapperFactory: Option[MapperFactory[Long]],
    indexStructure: IndexStructure,
    console: Boolean,
    kryoRegistrations: List[String]) extends JenaGraphAdapter with BlockingOperations with ConvenienceOperations with QueryEngine {
  import TripleRush._

  // Begin initialization ===========
  TrGlobal.dictionary = Some(dictionary)
  private[this] val scConfig = graphBuilder.config
  val numberOfNodes = {
    scConfig.preallocatedNodes.map(_.size).getOrElse(scConfig.nodeProvisioner.numberOfNodes)
  }
  val actorNamePrefix = scConfig.actorNamePrefix
  private[this] val signalCollectGraphBuilder = graphBuilder.
    withConsole(console).
    withKryoInitializer("com.signalcollect.triplerush.serialization.TripleRushKryoInit").
    withKryoRegistrations(kryoRegistrations).
    withMessageBusFactory(new CombiningMessageBusFactory(8096, 1024)).
    withUndeliverableSignalHandlerFactory(new TripleRushUndeliverableSignalHandlerFactory(indexStructure)).
    withEdgeAddedToNonExistentVertexHandlerFactory(new TripleRushEdgeAddedToNonExistentVertexHandlerFactory(indexStructure)).
    withMapperFactory(
      tripleMapperFactory.getOrElse {
        if (numberOfNodes > 1) {
          DistributedTripleMapperFactory
        } else {
          SingleNodeTripleMapperFactory
        }
      }).
      withStorageFactory(TripleRushStorage).
      withThrottlingEnabled(false).
      withThrottlingDuringLoadingEnabled(true).
      withWorkerFactory(new TripleRushWorkerFactory[Any]).
      withBlockingGraphModificationsSupport(false).
      withStatsReportingInterval(500).
      withEagerIdleDetection(false)
  val graph = signalCollectGraphBuilder.build
  private[this] val system = graph.system
  private[this] val cluster = Cluster(system)
  val log = system.log
  private[this] val noOperationsWhenShutdownMessage = "TripleRush has shut down, cannot execute this operation."
  private[this] var _isShutdown = false
  cluster.registerOnMemberRemoved {
    if (!_isShutdown) {
      val msg = "Cluster unexpectedly lost a member, TripleRush can no longer operate and is shutting down."
      println(msg)
      log.error(new Exception(msg), msg)
      close()
    }
  }
  private[this] val tripleRushShutdownPromise: Promise[Nothing] = Promise[Nothing]()
  protected val tripleRushShutdown: Future[Nothing] = tripleRushShutdownPromise.future // Future is failed when TripleRush shuts down.
  if (indexStructure.isSupported(Root)) {
    graph.addVertex(new RootIndex)
  }
  graph.execute(ExecutionConfiguration().withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  private[this] val startupMessage = s"""
| TripleRush has finished initialization. Config:
|   - number of nodes: ${graph.numberOfNodes}
|   - number of workers: ${graph.numberOfWorkers}
|   - mapper factory: ${signalCollectGraphBuilder.config.mapperFactory.getClass.getSimpleName}
|   - dictionary: ${dictionary.getClass.getSimpleName}
|   - indexStructure: ${indexStructure.getClass.getSimpleName}
""".stripMargin
  log.debug(startupMessage)
  // End initialization =============

  def isShutdown = _isShutdown

  def asyncAddTriplePatterns(i: Iterator[TriplePattern]): Future[Unit] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    val promise = Promise[Unit]()
    val vertex = new TripleAdditionSynchronizationVertex(indexStructure, i, promise)
    graph.addVertex(vertex)
    promise.future
  }

  def asyncAddEncodedTriple(sId: Int, pId: Int, oId: Int): Future[Unit] = {
    assert(!_isShutdown, noOperationsWhenShutdownMessage)
    assert(sId > 0 && pId > 0 && oId > 0)
    val promise = Promise[Unit]()
    val vertex = new TripleAdditionSynchronizationVertex(indexStructure, Some(TriplePattern(sId, pId, oId)).iterator, promise)
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

  override def close(): Unit = {
    if (!_isShutdown) {
      super.close()
      _isShutdown = true
      dictionary.close()
      graph.shutdown()
      tripleRushShutdownPromise.complete(Failure(
        new Exception("Operation was not completed, because TripleRush has shut down, possibly due to a cluster node failure.")))
    }
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
