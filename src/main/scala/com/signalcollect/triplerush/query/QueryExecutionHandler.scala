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

import com.signalcollect.triplerush.Shard
import com.signalcollect.triplerush.util.Streamer
import com.typesafe.config.ConfigFactory
import akka.actor.{ Actor, ActorLogging, ActorRef, actorRef2Scala }
import akka.cluster.sharding.ShardRegion
import akka.contrib.pattern.ReceivePipeline
import akka.stream.actor.ActorPublisherMessage.Request

object QueryExecutionHandler extends Shard {

  case object Register

  case class RegisterForQuery(queryId: Int)

  case class RequestResultsForQuery(queryId: Int, count: Int)

  val bufferSizeKey = "triplerush.max-buffer-per-query"

  val maxBufferPerQuery = ConfigFactory.load().getInt(bufferSizeKey)

  def apply(): Actor = new QueryExecutionHandler

  case class BindingsForQuery(queryid: Int, resultBindings: Array[Int])

  case class Tickets(queryid: Int, numberOfTickets: Long)

  val idExtractor: ShardRegion.ExtractEntityId = {
    case BindingsForQuery(queryId, bindings)        => (queryId.toString, bindings)
    case Tickets(queryId, tickets)                  => (queryId.toString, tickets)
    case RegisterForQuery(queryId)                  => (queryId.toString, Register)
    case RequestResultsForQuery(queryId, requested) => (queryId.toString, requested)
  }

  // TODO: Choose query ID such that this will resolve to a shard that is located on the same node.
  def queryIdToShardId(indexId: Long): String = {
    ((indexId.hashCode & Int.MaxValue) % 100).toString
  }

  val shardResolver: ShardRegion.ExtractShardId = {
    case BindingsForQuery(queryId, _)               => queryIdToShardId(queryId)
    case Tickets(queryId, tickets)                  => queryIdToShardId(queryId)
    case RegisterForQuery(queryId)                  => queryIdToShardId(queryId)
    case RequestResultsForQuery(queryId, requested) => queryIdToShardId(queryId)
    case other =>
      throw new Exception(s"Could not resolve message $other for shard $name.")
  }

}

// TODO: Define timeout and terminate when it is reached.
final class QueryExecutionHandler extends Streamer[Array[Int]] with ActorLogging with ReceivePipeline {

  def queryId: String = self.path.name
  override def toString(): String = self.path.name

  def bufferSize = QueryExecutionHandler.maxBufferPerQuery

  var downstreamActor: Option[ActorRef] = None
  var requested: Int = 0

  var missingTickets: Long = QueryExecutionHandler.maxBufferPerQuery
  def availableTickets = queue.freeCapacity

  def receive = {
    case Streamer.DeliverFromQueue =>
      println(s"delivering math.min(demand=$requested, size=${queue.size}) items to $downstreamActor")
      downstreamActor.foreach { a =>
        val items = queue.batchTakeAtMost(requested)
        val sent = a ! items
        requested -= items.length
        if (queue.isEmpty && missingTickets == 0) {
          a ! Streamer.Completed
        }
      }
    case tickets: Long =>
      missingTickets -= tickets
      println(s"got $tickets tickets, missingTickets=$missingTickets")
      ReceivePipeline.HandledCompletely
    case QueryExecutionHandler.Register =>
      println(s"got registration from $sender")
      downstreamActor = Some(sender)
      ReceivePipeline.HandledCompletely
    case requestedItems: Int =>
      println(s"got request for $requestedItems items from $sender")
      requested += requestedItems
      downstreamActor = Some(sender)
      ReceivePipeline.HandledCompletely
  }

}
