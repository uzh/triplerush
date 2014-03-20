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
import com.signalcollect.triplerush.vertices.query.IndexQueryVertex
import com.signalcollect.triplerush.vertices.query.ResultBindingQueryVertex
import com.signalcollect.triplerush.vertices.query.ResultCountingQueryVertex
import com.signalcollect.triplerush.vertices.RootIndex
import com.signalcollect.triplerush.optimizers.CleverCardinalityOptimizer
import com.signalcollect.triplerush.optimizers.ExplorationOptimizerCreator
import com.signalcollect.GraphEditor

case class TripleRush(
  graphBuilder: GraphBuilder[Any, Any] = GraphBuilder,
  optimizerCreator: Function1[TripleRush, Option[Optimizer]] = ExplorationOptimizerCreator,
  console: Boolean = false) extends QueryEngine {

  var canExecute = false

  val graph = graphBuilder.withConsole(console).
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
      "com.signalcollect.triplerush.PlaceholderEdge",
      "com.signalcollect.triplerush.CardinalityRequest",
      "com.signalcollect.triplerush.CardinalityReply",
      "com.signalcollect.triplerush.PredicateStatsReply",
      "com.signalcollect.triplerush.ChildIdRequest",
      "com.signalcollect.triplerush.ChildIdReply",
      "com.signalcollect.triplerush.SubjectCountSignal",
      "com.signalcollect.triplerush.ObjectCountSignal",
      "Array[com.signalcollect.triplerush.TriplePattern]",
      "com.signalcollect.interfaces.SignalMessage$mcIJ$sp",
      "com.signalcollect.interfaces.AddEdge",
      "akka.actor.RepointableActorRef")).build
  graph.setUndeliverableSignalHandler(UndeliverableRerouter.handle _)
  graph.setEdgeAddedToNonExistentVertexHandler(NonExistentVertexHandler.createIndexVertex _)
  val system = ActorSystemRegistry.retrieve("SignalCollect").get
  implicit val executionContext = system.dispatcher
  graph.addVertex(new RootIndex)
  var optimizer: Option[Optimizer] = None

  def prepareExecution {
    graph.awaitIdle
    graph.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.ContinuousAsynchronous))
    graph.awaitIdle
    canExecute = true
    optimizer = optimizerCreator(this)
  }

  def loadNtriples(ntriplesFilename: String, placementHint: Option[Any] = None) {
    graph.modifyGraph(FileLoader.loadNtriplesFile(ntriplesFilename) _, placementHint)
  }

  def loadBinary(binaryFilename: String, placementHint: Option[Any] = None) {
    graph.loadGraph(BinarySplitLoader(binaryFilename), placementHint)
  }

  def addTriple(s: String, p: String, o: String) {
    val sId = Dictionary(s)
    val pId = Dictionary(p)
    val oId = Dictionary(o)
    addEncodedTriple(sId, pId, oId)
  }

  def addTriplePattern(tp: TriplePattern) {
    addEncodedTriple(tp.s, tp.p, tp.o)
  }

  def addEncodedTriple(sId: Int, pId: Int, oId: Int) {
    assert(sId > 0 && pId > 0 && oId > 0)
    val po = TriplePattern(0, pId, oId)
    val so = TriplePattern(sId, 0, oId)
    val sp = TriplePattern(sId, pId, 0)
    graph.addEdge(po, new PlaceholderEdge(sId))
    graph.addEdge(so, new PlaceholderEdge(pId))
    graph.addEdge(sp, new PlaceholderEdge(oId))
  }

  def executeCountingQuery(
    q: QuerySpecification,
    optimizer: Option[Optimizer] = Some(GreedyCardinalityOptimizer)): Future[Option[Long]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val resultCountPromise = Promise[Option[Long]]()
    graph.addVertex(new ResultCountingQueryVertex(q, resultCountPromise, optimizer))
    resultCountPromise.future
  }

  /**
   * Blocking version of 'executeIndexQuery'.
   */
  def childIdsForPattern(indexId: TriplePattern): Set[Int] = {
    val intSetFuture = executeIndexQuery(indexId)
    Await.result(intSetFuture, 7200.seconds)
  }

  def executeIndexQuery(indexId: TriplePattern): Future[Set[Int]] = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    assert(!indexId.isFullyBound, "There is no index vertex with this id, as the pattern is fully bound.")
    val childIdPromise = Promise[Set[Int]]()
    graph.addVertex(new IndexQueryVertex(indexId, childIdPromise))
    childIdPromise.future
  }

  def executeQuery(q: QuerySpecification): Traversable[Array[Int]] = {
    executeQuery(q, optimizer)
  }

  def executeQuery(q: QuerySpecification, optimizer: Option[Optimizer]): Traversable[Array[Int]] = {
    val (resultFuture, statsFuture) = executeAdvancedQuery(q, optimizer)
    val result = Await.result(resultFuture, 7200.seconds)
    result
  }

  def executeAdvancedQuery(
    q: QuerySpecification,
    optimizer: Option[Optimizer]): (Future[Traversable[Array[Int]]], Future[Map[Any, Any]]) = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val resultPromise = Promise[Traversable[Array[Int]]]()
    val statsPromise = Promise[Map[Any, Any]]()
    graph.addVertex(new ResultBindingQueryVertex(q, resultPromise, statsPromise, optimizer))
    (resultPromise.future, statsPromise.future)
  }

  def awaitIdle {
    graph.awaitIdle
  }

  def shutdown = {
    graph.awaitIdle
    graph.shutdown
    CardinalityCache.clear
    PredicateStatsCache.clear
    QueryIds.reset
  }

  def edgesPerIndexType: Map[String, Int] = {
    graph.aggregate(new EdgesPerIndexType)
  }

  def countVerticesByType: Map[String, Int] = {
    graph.aggregate(new CountVerticesByType)
  }

}
