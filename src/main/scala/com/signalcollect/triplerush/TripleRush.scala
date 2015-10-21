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
import com.signalcollect.configuration.{ ActorSystemRegistry, ExecutionMode }
import com.signalcollect.factory.scheduler.Throughput
import com.signalcollect.interfaces._
import com.signalcollect.nodeprovisioning.cluster.ClusterNodeProvisioner
import com.signalcollect.triplerush.dictionary._
import com.signalcollect.triplerush.handlers._
import com.signalcollect.triplerush.loading._
import com.signalcollect.triplerush.mapper._
import com.signalcollect.triplerush.sparql._
import com.signalcollect.triplerush.util._
import com.signalcollect.triplerush.vertices._
import com.signalcollect.triplerush.vertices.query._
import com.signalcollect.{ ExecutionConfiguration, GraphBuilder }
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.jena.graph.{ Triple => JenaTriple }
import org.apache.jena.riot.Lang
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ Await, Future, Promise }
import scala.reflect.ManifestFactory
import com.signalcollect.nodeprovisioning.NodeProvisioner
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

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

  TrGlobal.dictionary = Some(dictionary)

  val graph = graphBuilder.
    withConsole(console).
    withKryoInitializer("com.signalcollect.triplerush.serialization.TripleRushKryoInit").
    withKryoRegistrations(kryoRegistrations).
    withMessageBusFactory(new CombiningMessageBusFactory(8096, 1024)).
    withUndeliverableSignalHandlerFactory(TripleRushUndeliverableSignalHandlerFactory).
    withEdgeAddedToNonExistentVertexHandlerFactory(TripleRushEdgeAddedToNonExistentVertexHandlerFactory).
    withMapperFactory(
      tripleMapperFactory.getOrElse(
        if (graphBuilder.config.preallocatedNodes.isEmpty && graphBuilder.config.nodeProvisioner.numberOfNodes == 1) {
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
      withEagerIdleDetection(false).build

  implicit private[this] val executionContext = graph.system.dispatcher

  initialize()

  def initialize(): Unit = {
    graph.addVertex(new RootIndex)
    graph.execute(ExecutionConfiguration().withExecutionMode(ExecutionMode.ContinuousAsynchronous))
  }

  def asyncAddTriplePatterns(i: Iterator[TriplePattern]): Future[Unit] = {
    val promise = Promise[Unit]()
    val vertex = new BlockingTripleAdditionsVertex(i, promise)
    graph.addVertex(vertex)
    promise.future
  }

  def asyncAddEncodedTriple(sId: Int, pId: Int, oId: Int): Future[Unit] = {
    assert(sId > 0 && pId > 0 && oId > 0)
    val promise = Promise[Unit]()
    val vertex = new BlockingTripleAdditionsVertex(Some(TriplePattern(sId, pId, oId)).iterator, promise)
    graph.addVertex(vertex)
    promise.future
  }

  def asyncCount(
    q: Seq[TriplePattern],
    tickets: Long = Long.MaxValue): Future[Option[Long]] = {
    // Efficient counting query.
    val resultCountPromise = Promise[Option[Long]]()
    val v = new ResultCountingQueryVertex(q, tickets, resultCountPromise)
    graph.addVertex(v)
    resultCountPromise.future
  }

  def asyncGetIndexAt(indexId: Long): Future[Array[Int]] = {
    val childIdPromise = Promise[Array[Int]]()
    graph.addVertex(new IndexQueryVertex(indexId, childIdPromise))
    childIdPromise.future
  }

  def resultIteratorForQuery(
    query: Seq[TriplePattern],
    numberOfSelectVariables: Option[Int] = None,
    tickets: Long = Long.MaxValue): Iterator[Array[Int]] = {
    val selectVariables = numberOfSelectVariables.getOrElse(
      VariableEncoding.requiredVariableBindingsSlots(query))
    val resultIterator = new ResultIterator
    val queryVertex = new ResultIteratorQueryVertex(query, selectVariables, tickets, resultIterator)
    graph.addVertex(queryVertex)
    resultIterator
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
