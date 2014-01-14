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
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.GraphBuilder
import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import com.signalcollect.configuration.ActorSystemRegistry
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.interfaces.AggregationOperation
import com.signalcollect.triplerush.QueryParticle.arrayToParticle
import com.signalcollect.triplerush.vertices.QueryVertex
import com.signalcollect.triplerush.vertices.RootIndex
import com.signalcollect.triplerush.loading.FileLoader
import com.signalcollect.triplerush.loading.BinarySplitLoader
import com.signalcollect.triplerush.loading.EdgesPerIndexType
import com.signalcollect.triplerush.loading.CountVerticesByType

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
      "com.signalcollect.triplerush.Optimizer",
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
    g.modifyGraph(FileLoader.loadNtriplesFile(ntriplesFilename) _, placementHint)
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
    FileLoader.addTriple(tp, g)
  }

  /**
   * Slow, only use for debugging purposes.
   */
  def addEncodedTriple(sId: Int, pId: Int, oId: Int) {
    FileLoader.addTriple(TriplePattern(sId, pId, oId), g)
  }

  def executeQuery(q: QuerySpecification): Traversable[Array[Int]] = {
    val (resultFuture, statsFuture) = executeAdvancedQuery(q, Some(new CleverCardinalityOptimizer))
    val result = Await.result(resultFuture, 7200.seconds)
    result
  }

  def executeAdvancedQuery(
    q: QuerySpecification,
    optimizer: Option[Optimizer]): (Future[Traversable[Array[Int]]], Future[Map[Any, Any]]) = {
    assert(canExecute, "Call TripleRush.prepareExecution before executing queries.")
    val resultPromise = Promise[Traversable[Array[Int]]]()
    val statsPromise = Promise[Map[Any, Any]]()
    g.addVertex(new QueryVertex(q, resultPromise, statsPromise, optimizer))
    if (!q.unmatched.isEmpty) {
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
