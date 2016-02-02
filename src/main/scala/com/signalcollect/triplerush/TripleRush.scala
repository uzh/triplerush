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
import com.signalcollect.triplerush.query.Query

import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.sharding.ClusterSharding
import akka.pattern.ask
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import akka.util.Timeout

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
  Index.registerWithSystem(system)
  import Index._
  import system.dispatcher

  protected val indexRegion = ClusterSharding(system).shardRegion(Index.shardName)

  def addTriplePattern(triplePattern: TriplePattern): Future[Unit] = {
    val ancestorIds = indexStructure.ancestorIds(triplePattern)
    val additionFutures = for {
      parentId <- ancestorIds
      parentIndexType = IndexType(parentId)
      delta = triplePattern.parentIdDelta(parentId.toTriplePattern)
    } yield (indexRegion ? AddChildId(parentId.toString, delta))
    val future = Future.sequence(additionFutures.toSeq)
    future.map(_ => Unit)
  }

  def query(
    query: Seq[TriplePattern],
    numberOfSelectVariables: Int,
    tickets: Long = Long.MaxValue): Source[Array[Int], Unit] = {
    val queryActor = system.actorOf(Props(
      new Query(query: Seq[TriplePattern], tickets: Long, numberOfSelectVariables: Int)))
    Source.fromPublisher(ActorPublisher(queryActor))
  }

}
