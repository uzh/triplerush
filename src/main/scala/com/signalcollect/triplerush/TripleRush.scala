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

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import com.signalcollect.triplerush.EfficientIndexPattern.longToIndexPattern
import com.signalcollect.triplerush.index.FullIndex
import com.signalcollect.triplerush.index.Index
import com.signalcollect.triplerush.index.Index.AddChildId
import com.signalcollect.triplerush.index.IndexStructure
import com.signalcollect.triplerush.index.IndexType
import com.signalcollect.triplerush.query.OperationIds
import com.signalcollect.triplerush.query.Query
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.sharding.ClusterSharding
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.signalcollect.triplerush.query.Query.Initialize
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.actor.ActorRef
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription

trait TripleStore {

  def addTriplePattern(triplePattern: TriplePattern): Future[Unit]

  def query(
    query: Seq[TriplePattern],
    numberOfSelectVariables: Int,
    tickets: Long = Long.MaxValue): Source[Array[Int], Unit]

}

object TripleRush {

  def apply(
    system: ActorSystem = ClusterCreator.create(1).head,
    indexStructure: IndexStructure = FullIndex,
    timeout: FiniteDuration = 300.seconds): TripleRush = {
    new TripleRush(system, indexStructure, Timeout(timeout))
  }

}

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
    query: Seq[TriplePattern],
    numberOfSelectVariables: Int,
    tickets: Long = Long.MaxValue): Source[Array[Int], Unit] = {
    val queryId = OperationIds.nextId()
    println("Calling out to a query actor")
    val queryActorFuture = queryRegion ?
      Initialize(queryId, query: Seq[TriplePattern], tickets, numberOfSelectVariables)
    println("Waiting for query actor to reply")
    val queryActor = Await.result(queryActorFuture, timeout.duration).asInstanceOf[ActorRef]
    println(s"Got an answer from the query actor ${queryActor}")
    //queryActor ! "NONSENSE!"
    val publisher = ActorPublisher(queryActor)
//    val subscriber = new Subscriber[Any] {
//      def onSubscribe(s: Subscription): Unit = println(s"onSubscribe($s)"); Unit
//      def onNext(t: Any): Unit = println(s"onNext($t)"); Unit
//      def onError(t: Throwable): Unit = println(s"onError($t)"); Unit
//      def onComplete(): Unit = println(s"onComplete"); Unit
//    }
//    publisher.subscribe(subscriber)
    Source.fromPublisher(publisher)
  }

}

