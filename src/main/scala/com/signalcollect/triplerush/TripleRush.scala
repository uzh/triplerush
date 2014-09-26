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

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.GraphBuilder
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.triplerush.loading.BinarySplitLoader
import com.signalcollect.triplerush.loading.CountVerticesByType
import com.signalcollect.triplerush.loading.EdgesPerIndexType
import com.signalcollect.triplerush.loading.FileLoader
import com.signalcollect.triplerush.optimizers.GreedyCardinalityOptimizer
import com.signalcollect.triplerush.optimizers.Optimizer
import com.signalcollect.triplerush.vertices.RootIndex
import com.signalcollect.triplerush.vertices.query.IndexQueryVertex
import com.signalcollect.triplerush.vertices.query.ResultBindingQueryVertex
import com.signalcollect.triplerush.vertices.query.ResultCountingQueryVertex
import com.signalcollect.triplerush.optimizers.ExplorationOptimizerCreator
import com.signalcollect.triplerush.util.ResultIterator
import com.signalcollect.triplerush.vertices.query.ResultIteratorQueryVertex
import com.signalcollect.triplerush.util.ArrayOfArraysTraversable
import com.signalcollect.triplerush.sparql.VariableEncoding
import com.signalcollect.triplerush.util.TripleRushStorage
import com.signalcollect.GraphBuilder
import com.signalcollect.triplerush.util.TripleRushWorkerFactory
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.interfaces.MapperFactory
import com.signalcollect.triplerush.vertices.query.QueryPlanningResult
import com.signalcollect.triplerush.vertices.query.QueryPlanningVertex
import java.util.concurrent.atomic.AtomicReference
import com.signalcollect.triplerush.loading.NtriplesLoader

/**
 * Global accessors for the console visualization.
 */
object TrGlobal {
  var dictionary: Option[Dictionary] = None
}

case class TripleRush(
  graphBuilder: GraphBuilder[Long, Any] = new GraphBuilder[Long, Any](),
  optimizerCreator: Function1[TripleRush, Option[Optimizer]] = ExplorationOptimizerCreator,
  val dictionary: Dictionary = new CompressedDictionary(),
  tripleMapperFactory: Option[MapperFactory[Long]] = None,
  console: Boolean = false) extends QueryEngine {

  TrGlobal.dictionary = Some(dictionary)

  var canExecute = false

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
      withWorkerFactory(new TripleRushWorkerFactory[Any]).
      withBlockingGraphModificationsSupport(false).
      withHeartbeatInterval(500).
      withEagerIdleDetection(false).
      withKryoRegistrations(List(
        "com.signalcollect.triplerush.vertices.RootIndex",
        "com.signalcollect.triplerush.vertices.SIndex",
        "com.signalcollect.triplerush.vertices.PIndex",
        "com.signalcollect.triplerush.vertices.OIndex",
        "com.signalcollect.triplerush.vertices.SPIndex",
        "com.signalcollect.triplerush.vertices.POIndex",
        "com.signalcollect.triplerush.vertices.SOIndex",
        "com.signalcollect.triplerush.TriplePattern",
        "com.signalcollect.triplerush.PlaceholderEdge",
        "com.signalcollect.triplerush.CardinalityRequest",
        "com.signalcollect.triplerush.CardinalityReply",
        "com.signalcollect.triplerush.PredicateStatsReply",
        "com.signalcollect.triplerush.ChildIdRequest",
        "com.signalcollect.triplerush.ChildIdReply",
        "com.signalcollect.triplerush.SubjectCountSignal",
        "com.signalcollect.triplerush.ObjectCountSignal",
        "Array[com.signalcollect.triplerush.TriplePattern]",
        "com.signalcollect.interfaces.AddEdge",
        "com.signalcollect.triplerush.CombiningMessageBusFactory",
        "com.signalcollect.triplerush.SingleNodeTripleMapperFactory$",
        "com.signalcollect.triplerush.DistributedTripleMapperFactory$",
        "com.signalcollect.triplerush.LoadBalancingTripleMapperFactory$",
        "com.signalcollect.triplerush.AlternativeTripleMapperFactory",
        "com.signalcollect.triplerush.PredicateStats",
        "com.signalcollect.triplerush.vertices.query.ResultIteratorQueryVertex", // Only for local serialization test.
        "com.signalcollect.triplerush.util.ResultIterator", // Only for local serialization test.
        "java.util.concurrent.atomic.AtomicBoolean", // Only for local serialization test.
        "java.util.concurrent.LinkedBlockingQueue", // Only for local serialization test.
        "scala.reflect.ManifestFactory$$anon$10",
        "com.signalcollect.triplerush.util.TripleRushStorage$",
        "akka.actor.RepointableActorRef",
        "com.signalcollect.triplerush.TripleRushEdgeAddedToNonExistentVertexHandlerFactory$",
        "com.signalcollect.factory.scheduler.Throughput$mcJ$sp",
        "com.signalcollect.triplerush.TripleRushUndeliverableSignalHandlerFactory$",
        "com.signalcollect.triplerush.util.TripleRushWorkerFactory",
        "com.signalcollect.interfaces.BulkSignalNoSourceIds$mcJ$sp",
        "com.signalcollect.interfaces.SignalMessageWithoutSourceId$mcJ$sp")).build
  val system = ActorSystemRegistry.retrieve("SignalCollect").get
  implicit val executionContext = system.dispatcher
  graph.addVertex(new RootIndex)
  var optimizer: Option[Optimizer] = None

  def prepareExecution {
    graph.awaitIdle
    graph.execute(ExecutionConfiguration().withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    graph.awaitIdle
    canExecute = true
    optimizer = optimizerCreator(this)
  }

  /**
   * The placement hint should ensure that this gets processed on node 0, because the dictionary resides on that node.
   * If you get a serialization error for the dictionary, it is probably due to a problematic placement hint.
   */
  def loadNtriples(ntriplesFilename: String, placementHint: Option[Long] = Some(QueryIds.embedQueryIdInLong(1))) {
    graph.loadGraph(new NtriplesLoader(ntriplesFilename, dictionary), placementHint)
  }

  def loadBinary(binaryFilename: String, placementHint: Option[Long] = None) {
    graph.loadGraph(BinarySplitLoader(binaryFilename), placementHint)
  }

  def addTriple(s: String, p: String, o: String) {
    val sId = dictionary(s)
    val pId = dictionary(p)
    val oId = dictionary(o)
    addEncodedTriple(sId, pId, oId)
  }

  def addTriplePattern(tp: TriplePattern) {
    addEncodedTriple(tp.s, tp.p, tp.o)
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int) {
    assert(sId > 0 && pId > 0 && oId > 0)
    val po = EfficientIndexPattern(0, pId, oId)
    val so = EfficientIndexPattern(sId, 0, oId)
    val sp = EfficientIndexPattern(sId, pId, 0)
    graph.addEdge(po, new PlaceholderEdge(sId))
    graph.addEdge(so, new PlaceholderEdge(pId))
    graph.addEdge(sp, new PlaceholderEdge(oId))
  }

  def executeCountingQuery(
    q: Seq[TriplePattern],
    optimizer: Option[Optimizer] = Some(GreedyCardinalityOptimizer),
    tickets: Long = Long.MaxValue): Future[Option[Long]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    // Efficient counting query.
    val resultCountPromise = Promise[Option[Long]]()
    graph.addVertex(new ResultCountingQueryVertex(q, tickets, resultCountPromise, optimizer))
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

  def executeQuery(q: Seq[TriplePattern]): Traversable[Array[Int]] = {
    executeQuery(q, optimizer)
  }

  def executeQuery(q: Seq[TriplePattern], optimizer: Option[Optimizer] = None, tickets: Long = Long.MaxValue): Traversable[Array[Int]] = {
    val (resultFuture, statsFuture) = executeAdvancedQuery(q, optimizer, tickets = tickets)
    val result = Await.result(resultFuture, 7200.seconds)
    result
  }

  def getQueryPlan(query: Seq[TriplePattern], optimizerOption: Option[Optimizer] = None): QueryPlanningResult = {
    val resultPromise = Promise[QueryPlanningResult]()
    val usedOptimizer = optimizerOption.getOrElse(optimizer.get)
    val queryVertex = new QueryPlanningVertex(query, resultPromise, usedOptimizer)
    graph.addVertex(queryVertex)
    val result = Await.result(resultPromise.future, 7200.seconds)
    result
  }

  /**
   * If the optimizer is defined, uses that one, else uses the default.
   */
  def executeAdvancedQuery(
    query: Seq[TriplePattern],
    optimizerOption: Option[Optimizer] = None,
    numberOfSelectVariables: Option[Int] = None,
    tickets: Long = Long.MaxValue): (Future[Traversable[Array[Int]]], Future[Map[Any, Any]]) = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val selectVariables = numberOfSelectVariables.getOrElse(
      VariableEncoding.requiredVariableBindingsSlots(query))
    val resultPromise = Promise[Traversable[Array[Int]]]()
    val statsPromise = Promise[Map[Any, Any]]()
    val usedOptimizer = if (optimizerOption.isDefined) optimizerOption else optimizer
    val queryVertex = new ResultBindingQueryVertex(query, selectVariables, tickets, resultPromise, statsPromise, usedOptimizer)
    graph.addVertex(queryVertex)
    (resultPromise.future, statsPromise.future)
  }

  def resultIteratorForQuery(
    query: Seq[TriplePattern]) = resultIteratorForQuery(query, None, None, Long.MaxValue)

  /**
   * If the optimizer is defined, uses that one, else uses the default.
   */
  def resultIteratorForQuery(
    query: Seq[TriplePattern],
    optimizerOption: Option[Optimizer] = None,
    numberOfSelectVariables: Option[Int] = None,
    tickets: Long = Long.MaxValue): Iterator[Array[Int]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val selectVariables = numberOfSelectVariables.getOrElse(
      VariableEncoding.requiredVariableBindingsSlots(query))
    val resultIterator = new ResultIterator
    val usedOptimizer = if (optimizerOption.isDefined) optimizerOption else optimizer
    val queryVertex = new ResultIteratorQueryVertex(query, selectVariables, tickets, resultIterator, usedOptimizer)
    graph.addVertex(queryVertex)
    resultIterator
  }

  def awaitIdle {
    graph.awaitIdle
  }

  def clear {
    clearCaches
    clearDictionary
    graph.reset
    graph.awaitIdle
    graph.addVertex(new RootIndex)
  }

  def clearCaches {
    graph.awaitIdle
    CardinalityCache.clear
    PredicateStatsCache.clear
    QueryIds.reset
  }

  def clearDictionary {
    dictionary.clear
  }

  def shutdown = {
    clearCaches
    clearDictionary
    graph.shutdown
  }

  def edgesPerIndexType: Map[String, Int] = {
    graph.aggregate(new EdgesPerIndexType)
  }

  def countVerticesByType: Map[String, Int] = {
    graph.aggregate(new CountVerticesByType)
  }

}
