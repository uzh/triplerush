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

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.index.{ FullIndex, Index }
import com.signalcollect.triplerush.index.{ IndexStructure, IndexType }
import com.signalcollect.triplerush.index.Index.AddChildId
import com.signalcollect.triplerush.query.{ OperationIds, Query }
import com.signalcollect.triplerush.query.Query.Initialize
import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import akka.stream.actor.AbstractActorPublisher
import akka.cluster.Cluster
import java.util.concurrent.CountDownLatch

object TripleRush {

  def apply(
    system: ActorSystem,
    indexStructure: IndexStructure = FullIndex,
    timeout: FiniteDuration = 300.seconds): TripleRush = {
    val latch = new CountDownLatch(1)
    Cluster(system).registerOnMemberUp {
      latch.countDown()
    }
    latch.await()
    new TripleRush(system, indexStructure, Timeout(timeout))
  }

}

/**
 * Assumes that the whole cluster has already started.
 */
class TripleRush(system: ActorSystem,
                 indexStructure: IndexStructure,
                 implicit protected val timeout: Timeout) extends TripleStore {
  import system.dispatcher

  protected val indexRegion = Index.shard(system)
  protected val queryRegion = Query.shard(system)

  // TODO: Make efficient by building the index structure recursively.
  def addTriplePattern(triplePattern: TriplePattern): Future[Unit] = {
    val ancestorIds = indexStructure.ancestorIds(triplePattern)
    val additionFutures = for {
      parentId <- ancestorIds
      parentIndexType = IndexType(parentId)
      delta = triplePattern.parentIdDelta(parentId.toTriplePattern)
    } yield (indexRegion ? AddChildId(parentId, delta))
    val future = Future.sequence(additionFutures)
    future.map(_ => Unit)
  }

  // TODO: `ActorPublisher` does not support failure handling for distributed use cases yet.
  // TODO: Clean up when a timeout is encountered.
  def query(
    query: Vector[TriplePattern],
    numberOfSelectVariables: Int,
    tickets: Long = Long.MaxValue): Source[Array[Int], Unit] = {
    val queryId = OperationIds.nextId()
    val queryActorFuture = queryRegion ?
      Initialize(queryId, query: Seq[TriplePattern], tickets, numberOfSelectVariables)
    val queryActor = Await.result(queryActorFuture, timeout.duration).asInstanceOf[ActorRef]
    val publisher = ActorPublisher(queryActor)
    Source.fromPublisher(publisher)
  }

  def close(): Unit = {}

}
