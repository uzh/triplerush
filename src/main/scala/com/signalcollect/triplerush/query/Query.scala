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

package com.signalcollect.triplerush.query

import scala.annotation.tailrec
import scala.collection.immutable.Queue

import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.index.Index
import com.signalcollect.triplerush.query.QueryParticle.arrayToParticle

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.cluster.sharding.ClusterSharding
import akka.event.LoggingReceive
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request

object Query {

  val emptyQueue = Queue.empty[Array[Int]]

}

class Query(
    queryId: Int,
    query: Seq[TriplePattern],
    tickets: Long,
    numberOfSelectVariables: Int) extends ActorPublisher[Array[Int]] with ActorLogging {

  assert(numberOfSelectVariables > 0)

  private[this] var receivedTickets: Long = 0

  def sendToIndex(indexId: Long, message: Any): Unit = {
    val indexShard = ClusterSharding(context.system).shardRegion(Index.shardName)
    indexShard ! message
  }

  override def preStart() {
    log.info(s"Query actor $queryId `preStart`")
    if (query.length > 0) {
      val particle = QueryParticle(
        patterns = query,
        queryId = queryId,
        numberOfSelectVariables = numberOfSelectVariables,
        tickets = tickets)
      sendToIndex(particle.routingAddress, particle)
      log.info(s"Query actor $queryId sent out ${ParticleDebug(particle).toString}")
    } else {
      // No patterns, no results, send all tickets to ourself immediately.
      self ! tickets
    }
  }

  def receive: Actor.Receive = queuing(Query.emptyQueue, 0)

  /**
   * Stores both the queued result bindings and how many tickets have been received.
   */
  def queuing(queued: Queue[Array[Int]], receivedTickets: Long): Actor.Receive = LoggingReceive {
    case bindings: Array[Int] =>
      if (queued.isEmpty) {
        onNext(bindings)
        context.become(queuing(Query.emptyQueue, receivedTickets))
      } else {
        val updatedQueue = queued.enqueue(bindings)
        val remainingQueue = deliverFromQueue(updatedQueue, receivedTickets == tickets)
        context.become(queuing(remainingQueue, receivedTickets))
      }
    case tickets: Long =>
      context.become(queuing(queued, receivedTickets + tickets))
      onCompleteThenStop()
    case Request(cnt) =>
      val remaining = deliverFromQueue(queued, receivedTickets == tickets)
      context.become(queuing(remaining, receivedTickets))
    case Cancel =>
      context.stop(self)
  }

  /**
   * Delivers from `queued' whatever it can, then returns a queue with the remaining items.
   * Completes the stream if all results were delivered and the query execution has completed.
   */
  @tailrec private[this] def deliverFromQueue(queued: Queue[Array[Int]], completed: Boolean): Queue[Array[Int]] = {
    if (totalDemand >= queued.size) {
      queued.foreach(onNext)
      if (completed) {
        onCompleteThenStop()
      }
      Query.emptyQueue
    } else if (totalDemand == 0) {
      queued
    } else {
      if (totalDemand <= Int.MaxValue) {
        val (toDeliver, remaining) = queued.splitAt(totalDemand.toInt)
        toDeliver.foreach(onNext)
        remaining
      } else {
        val (toDeliver, remaining) = queued.splitAt(Int.MaxValue)
        toDeliver.foreach(onNext)
        deliverFromQueue(remaining, completed)
      }
    }
  }

}
