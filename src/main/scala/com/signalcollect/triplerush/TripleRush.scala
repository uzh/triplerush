/*
 * Copyright (C) 2015 Cotiviti Labs (nexgen.admin@cotiviti.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.signalcollect.triplerush

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import scala.util.{ Failure, Success }
import com.signalcollect.triplerush.query.{ OperationIds, QueryExecutionHandler, VariableEncoding }
import com.signalcollect.triplerush.result.LocalResultStreamer
import akka.NotUsed
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.util.Timeout
import com.signalcollect.triplerush.index.Index.AddEdge
import com.signalcollect.triplerush.index.Index
import com.signalcollect.triplerush.index.Outgoing
import com.signalcollect.triplerush.index.Incoming

object TripleRush {

  def apply(
    system: ActorSystem,
    timeout: FiniteDuration = 300.seconds): TripleRush = {
    val latch = new CountDownLatch(1)
    Cluster(system).registerOnMemberUp {
      latch.countDown()
    }
    latch.await()
    new TripleRush(system, Timeout(timeout))
  }

}

/**
 * Assumes that the whole cluster has already started.
 */
class TripleRush(system: ActorSystem,
                 implicit protected val timeout: Timeout) extends TripleStore {
  import system.dispatcher

  protected val indexRegion = Index.shard(system)
  protected val queryRegion = QueryExecutionHandler.shard(system)
  protected val parallelism = Runtime.getRuntime.availableProcessors
  protected implicit val materializer = ActorMaterializer.create(system)

  protected val triplePatternLoader = {
    Flow[TriplePattern]
      .mapAsyncUnordered(parallelism) { triplePattern =>
        val s = triplePattern.s
        val p = triplePattern.p
        val o = triplePattern.o
        val subjectAdditionFuture = indexRegion ? AddEdge(s, Outgoing(p, o))
        val objectAdditionFuture = indexRegion ? AddEdge(o, Incoming(p, s))
        val future = Future.sequence(List(subjectAdditionFuture, objectAdditionFuture))
        Await.ready(future.map(_ => NotUsed), 120.seconds)
      }
  }

  def addTriplePatterns(triplePatterns: Source[TriplePattern, NotUsed]): Future[NotUsed] = {
    val promise = Promise[NotUsed]()
    val graph = triplePatterns
      .via(triplePatternLoader)
      .to(Sink.onComplete {
        case Success(_) => promise.success(NotUsed)
        case Failure(e) => promise.failure(e)
      }).run()
    promise.future
  }

  // TODO: Clean up when a timeout is encountered.
  def query(query: Vector[TriplePattern]): Source[Bindings, NotUsed] = {
    val numberOfSelectVariables = VariableEncoding.requiredVariableBindingsSlots(query)
    val queryId = OperationIds.nextId()
    val localResultStreamer = system.actorOf(
      LocalResultStreamer.props(queryId, query, tickets = QueryExecutionHandler.maxBufferPerQuery, numberOfSelectVariables))
    val publisher = ActorPublisher(localResultStreamer)
    // TODO: Set `subscriptionTimeout`.
    Source.fromPublisher(publisher)
  }

  def close(): Unit = {}

}
