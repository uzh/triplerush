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

import com.signalcollect.triplerush.{ Shard, TriplePattern }
import com.signalcollect.triplerush.index.Index
import com.signalcollect.triplerush.query.Query.{ Initialize, emptyQueue }
import com.signalcollect.triplerush.query.QueryParticle.arrayToParticle

import akka.actor.{ Actor, ActorLogging, Props, actorRef2Scala }
import akka.cluster.sharding.{ ClusterSharding, ShardRegion }
import akka.event.LoggingReceive
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request

object Query extends Shard {

  def apply(): Actor = new Query

  val emptyQueue = Queue.empty[Array[Int]]

  case class Initialize(
    queryId: Int,
    query: Seq[TriplePattern],
    tickets: Long,
    numberOfSelectVariables: Int)

  case class BindingsForQuery(queryid: Int, resultBindings: Array[Int])

  case class Tickets(queryid: Int, numberOfTickets: Long)

  val idExtractor: ShardRegion.ExtractEntityId = {
    case initialize @ Initialize(queryId, _, _, _) => (queryId.toString, initialize)
    case BindingsForQuery(queryId, bindings)       => (queryId.toString, bindings)
    case Tickets(quryId, tickets)                  => (quryId.toString, tickets)
  }

  // TODO: Choose query ID such that this will resolve to a shard that is located on the same node.
  def queryIdToShardId(indexId: Long): String = {
    ((indexId.hashCode & Int.MaxValue) % 100).toString
  }

  val shardResolver: ShardRegion.ExtractShardId = {
    case initialize @ Initialize(queryId, _, _, _) => queryIdToShardId(queryId)
    case BindingsForQuery(queryId, _)              => queryIdToShardId(queryId)
    case Tickets(quryId, tickets)                  => queryIdToShardId(quryId)
  }

}

final class Query() extends ActorPublisher[Array[Int]] with ActorLogging {

  def queryId: String = self.path.name
  override def toString(): String = {
    queryId.toInt.toString
  }

  def sendToIndex(indexId: Long, message: Any): Unit = {
    Index.shard(context.system) ! message
  }

  override def preStart(): Unit = {
    log.info(s"Query actor $toString was started with path ${context.self}.")
  }

  def receive: Actor.Receive = LoggingReceive {
    case Initialize(queryId, query, tickets, numberOfSelectVariables) =>
      assert(numberOfSelectVariables > 0)
      log.info(s"Query actor $queryId `preStart`")
      if (query.length > 0) {
        val particle = QueryParticle(
          patterns = query,
          queryId = queryId,
          numberOfSelectVariables = numberOfSelectVariables,
          tickets = tickets)
        sendToIndex(particle.routingAddress, particle)
        sender() ! self
        log.info(s"Query actor $queryId sent out ${ParticleDebug(particle).toString}")
        context.become(resultStreaming(emptyQueue, tickets))
      } else {
        // No patterns, no results, complete stream immediately.
        deliverFromQueue(emptyQueue, completed = true)
      }
    case other: Any =>
      println(s"OOOOPS, query actor got $other when awaiting initialization")
  }

  /**
   * Stores both the queued result bindings and how many tickets have been received.
   */
  def resultStreaming(queued: Queue[Array[Int]], missingTickets: Long): Actor.Receive = LoggingReceive {
    case bindings: Array[Int] =>
      log.info(s"Query actor $toString with path ${context.self} received bindings ${bindings.mkString("[", ")", "]")}.")
      if (queued.isEmpty && totalDemand > 0) {
        onNext(bindings)
        context.become(resultStreaming(emptyQueue, missingTickets))
      } else {
        val updatedQueue = queued.enqueue(bindings)
        val remainingQueue = deliverFromQueue(updatedQueue, missingTickets == 0)
        context.become(resultStreaming(remainingQueue, missingTickets))
      }
    case tickets: Long =>
      val remaining = deliverFromQueue(queued, missingTickets == tickets)
      context.become(resultStreaming(remaining, missingTickets - tickets))
    case Request(count) =>
      println(s"Received request for $count")
      val remaining = deliverFromQueue(queued, missingTickets == 0)
      context.become(resultStreaming(remaining, missingTickets))
    case other: Any =>
      println(s"OOOOPS, query actor got $other when in streaming")
  }

  /**
   * Delivers from `queued' whatever it can, then returns a queue with the remaining items.
   * Completes the stream if all results were delivered and the query execution has completed.
   */
  @tailrec private[this] def deliverFromQueue(queued: Queue[Array[Int]], completed: Boolean): Queue[Array[Int]] = {
    if (completed && queued == emptyQueue) {
      println(s"Completing stream!")
      onCompleteThenStop()
      emptyQueue
    } else if (totalDemand >= queued.size) {
      println(s"Streaming ${math.min(totalDemand, queued.size)} items!")
      queued.foreach(onNext)
      if (completed) {
        onCompleteThenStop()
      }
      emptyQueue
    } else if (totalDemand == 0) {
      println(s"No demand, waiting! Queued = $queued.")
      queued
    } else if (totalDemand <= Int.MaxValue) {
      val (toDeliver, remaining) = queued.splitAt(totalDemand.toInt)
      println(s"Streaming ${toDeliver.size} items!")
      toDeliver.foreach(onNext)
      remaining
    } else {
      // TODO: Catch this case by ensuring that we deliver if total demand > queue.size.
      // afterwards check if we need to deliver again.
      val (toDeliver, remaining) = queued.splitAt(Int.MaxValue)
      println(s"Streaming ${toDeliver.size} items!")
      toDeliver.foreach(onNext)
      deliverFromQueue(remaining, completed)
    }
  }

}
