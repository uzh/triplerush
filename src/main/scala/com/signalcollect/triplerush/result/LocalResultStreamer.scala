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
import com.signalcollect.triplerush.query.QueryParticle
import com.signalcollect.triplerush.query.QueryParticle.arrayToParticle

import akka.actor.actorRef2Scala

object LocalResultStreamer {

  case object Completed

  val emptyQueue = Queue.empty[Array[Int]]

  case class Initialize()

}

// TODO: Define timeout and terminate when it is reached.
// TODO: Max queue size? What to do when full?
final class LocalResultStreamer(
    queryId: Int,
    query: Seq[TriplePattern],
    tickets: Long,
    numberOfSelectVariables: Int) extends Streamer[Array[Int]] {

  override def emptyQueue = ??? //LocalResultStreamer.emptyQueue

  def sendToIndex(indexId: Long, message: Any): Unit = {
    Index.shard(context.system) ! message
  }

  override def preStart(): Unit = {
    assert(numberOfSelectVariables > 0)
    if (query.length > 0) {
      val particle = QueryParticle(
        patterns = query,
        queryId = queryId,
        numberOfSelectVariables = numberOfSelectVariables,
        tickets = tickets)
      sendToIndex(particle.routingAddress, particle)
    } else {
      // No patterns, no results, set completed to true immediately.
      context.become(buffering(emptyQueue, completed = true))
    }
  }

}
