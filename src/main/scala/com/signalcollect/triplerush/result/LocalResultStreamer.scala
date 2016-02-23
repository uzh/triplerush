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

package com.signalcollect.triplerush.result

import scala.collection.immutable.Queue
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.index.Index
import com.signalcollect.triplerush.util.Streamer
import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import com.signalcollect.triplerush.query.QueryExecutionHandler.RegisterForQuery
import com.signalcollect.triplerush.query.QueryExecutionHandler.RequestResultsForQuery
import akka.contrib.pattern.ReceivePipeline
import akka.actor.util.Flush
import com.signalcollect.triplerush.Bindings
import com.signalcollect.triplerush.index.QueryParticle
import com.signalcollect.triplerush.query.QueryExecutionHandler

object LocalResultStreamer {

  val emptyQueue = Queue.empty[Array[Int]]

  def props(queryId: Int,
            query: Vector[TriplePattern],
            tickets: Long,
            numberOfSelectVariables: Int): Props = Props(
    new LocalResultStreamer(queryId, query, tickets, numberOfSelectVariables))

}

// TODO: Define timeout and terminate when it is reached.
// TODO: Max queue size? What to do when full?
final class LocalResultStreamer(
  queryId: Int,
  query: Vector[TriplePattern],
  tickets: Long,
  numberOfSelectVariables: Int)
    extends Streamer[Array[Int]] with ActorPublisher[Bindings] with ReceivePipeline {

  def bufferSize = QueryExecutionHandler.maxBufferPerQuery
  var completed = false

  def canDeliver: Boolean = {
    totalDemand > 0
  }

  override def preStart(): Unit = {
    assert(numberOfSelectVariables > 0)
    if (query.length == 0) {
      // No patterns, no results: complete immediately.
      self ! Streamer.Completed
    } else {
      val particle = QueryParticle(
        id = queryId,
        tickets = tickets,
        bindings = Map.empty[Int, Int],
        unmatched = query)
      QueryExecutionHandler.shard(context.system) ! RegisterForQuery(queryId)
      QueryExecutionHandler.shard(context.system) ! RequestResultsForQuery(queryId, queue.freeCapacity)
      println(s"Sending $particle on its merry way.")
      Index.shard(context.system) ! particle
    }
  }

  def receive = {
    case Streamer.DeliverFromQueue =>
      println(s"local result streamer $self got DeliverFromQueue(size=${queue.size}, totalDemand=$totalDemand)")
      if (totalDemand < queue.size) {
        // Safe conversion to Int, because smaller than queue size.
        queue.batchProcessAtMost(totalDemand.toInt, { i => println(s"local result streamer is onNext'ing $i"); onNext(new Bindings(i)) })
      } else {
        // Queue empty afterwards, if completed, we're done, else we'd get more
        // results again before needing to take action.
        queue.batchProcessAtMost(queue.size, { i => println(s"local result streamer is onNext'ing $i"); onNext(new Bindings(i)) })
      }
      if (completed && queue.isEmpty) {
        println(s"$self is completing the outgoing stream")
        onCompleteThenStop()
      }
    case Streamer.Completed =>
      println(s"local result streamer $self got completed message from upstream")
      completed = true
      if (queue.isEmpty) {
        println(s"$self is completing the outgoing stream")
        onCompleteThenStop()
      }
  }

}
