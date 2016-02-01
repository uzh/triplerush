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

import akka.actor.{ ActorLogging, ActorSystem, Props }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.persistence.PersistentActor
import akka.actor.actorRef2Scala
import com.signalcollect.triplerush.IntSet
import com.signalcollect.triplerush.SimpleIntSet
import com.signalcollect.triplerush.EfficientIndexPattern._
import akka.actor.Actor
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.ResultIterator
import akka.stream.actor.ActorPublisher
import scala.collection.mutable.Buffer
import akka.event.LoggingReceive
import scala.collection.immutable.Queue
import akka.stream.actor.ActorPublisherMessage.Cancel
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import scala.annotation.tailrec

object Query {

  val emptyQueue = Queue.empty[Array[Int]]

}

class Query(
    query: Seq[TriplePattern],
    tickets: Long,
    numberOfSelectVariables: Int) extends ActorPublisher[Array[Int]] with ActorLogging {

  assert(numberOfSelectVariables > 0)

  private[this] var receivedTickets: Long = 0

  private[this] val MaxQueueSize = 1000000

  def receive: Actor.Receive = queuing(Query.emptyQueue, false)

  /**
   * Queuing mode that stores both the queued elements and if the stream has been completed.
   */
  def queuing(queued: Queue[Array[Int]], receivedTickets: Long, completed: Boolean): Actor.Receive = LoggingReceive {
    case bindings: Array[Int] =>
      if (queued.isEmpty) {
        onNext(bindings)
        context.become(queuing(Query.emptyQueue, receivedTickets, completed))
      } else {
        val updatedQueue = queued.enqueue(bindings)
        val remainingQueue = deliverFromQueue(updatedQueue, completed)
        context.become(queuing(remainingQueue, receivedTickets, completed))
      }
    case tickets: Long =>
      receivedTickets += deliveredTickets
      if (buffer.isEmpty && tickets == receivedTickets) {
        onCompleteThenStop()
      }
    case Request(cnt) =>
      val remaining = deliverFromQueue(queued, completed)
      context.become(queuing(remaining, completed))
    case Cancel =>
      context.stop(self)
  }

  /**
   * Delivers from `q' whatever it can, then returns a queue with the remaining items.
   * Completes if all items were delivered and the stream has completed.
   */
  @tailrec private[this] def deliverFromQueue(q: Queue[Array[Int]], completed: Boolean): Queue[Array[Int]] = {
    if (totalDemand >= q.size) {
      q.foreach(onNext)
      if (completed) {
        onCompleteThenStop()
      }
      Query.emptyQueue
    } else if (totalDemand == 0) {
      q
    } else {
      if (totalDemand <= Int.MaxValue) {
        val (toDeliver, remaining) = q.splitAt(totalDemand.toInt)
        toDeliver.foreach(onNext)
        remaining
      } else {
        val (toDeliver, remaining) = q.splitAt(Int.MaxValue)
        toDeliver.foreach(onNext)
        deliverFromQueue(remaining, completed)
      }
    }
  }

  //  val MaxBufferSize = 1000000
  //  val buffer = new collection.mutable.Queue[Array[Int]]
  //
  //  def receive = {
  //    case deliveredTickets: Long =>
  //      receivedTickets += deliveredTickets
  //      if (buffer.isEmpty && tickets == receivedTickets) {
  //        onCompleteThenStop()
  //      }
  //    case bindings: Array[Int] =>
  //      if (buffer.isEmpty && totalDemand > 0) {
  //        onNext(bindings)
  //      } else {
  //        buffer += bindings
  //      }
  //  }

  //  def receive = {
  //    case job: Job if buf.size == MaxBufferSize =>
  //      sender() ! JobDenied
  //    case job: Job =>
  //      sender() ! JobAccepted
  //      if (buf.isEmpty && totalDemand > 0)
  //        onNext(job)
  //      else {
  //        buf :+= job
  //        deliverBuf()
  //      }
  //    case Request(_) =>
  //      deliverBuf()
  //    case Cancel =>
  //      context.stop(self)
  //  }
  // 
  //  @tailrec final def deliverBuf(): Unit =
  //    if (totalDemand > 0) {
  //      /*
  //       * totalDemand is a Long and could be larger than
  //       * what buf.splitAt can accept
  //       */
  //      if (totalDemand <= Int.MaxValue) {
  //        val (use, keep) = buf.splitAt(totalDemand.toInt)
  //        buf = keep
  //        use foreach onNext
  //      } else {
  //        val (use, keep) = buf.splitAt(Int.MaxValue)
  //        buf = keep
  //        use foreach onNext
  //        deliverBuf()
  //      }
  //    }

  //  private[this] val emptyQueue = Queue.empty[C]
  //
  //  /**
  //   * Queuing mode that stores both the queued elements and if the stream has been completed.
  //   */
  //  def queuing(queued: Queue[C], completed: Boolean): Actor.Receive = LoggingReceive {
  //    case OnNext(e: C) =>
  //      val updatedQueue = queued.enqueue(e)
  //      val remainingQueue = deliverFromQueue(updatedQueue, completed)
  //      context.become(queuing(remainingQueue, completed))
  //    case q: Queue[C] =>
  //      val updatedQueue = queued.enqueue(q)
  //      val remainingQueue = deliverFromQueue(updatedQueue, completed)
  //      context.become(queuing(remainingQueue, completed))
  //    case Request(cnt) =>
  //      val remaining = deliverFromQueue(queued, completed)
  //      context.become(queuing(remaining, completed))
  //    case Complete =>
  //      val remaining = deliverFromQueue(queued, true)
  //      context.become(queuing(remaining, true))
  //    case Cancel =>
  //      context.stop(self)
  //  }
  //
  //  def receive: Actor.Receive = queuing(emptyQueue, false)
  //
  //  /**
  //   * Delivers from `q' whatever it can, then returns a queue with the remaining items.
  //   * Completes if all items were delivered and the stream has completed.
  //   */
  //  def deliverFromQueue(q: Queue[C], completed: Boolean): Queue[C] = {
  //    if (totalDemand >= q.size) {
  //      q.foreach(onNext(_))
  //      if (completed) {
  //        onComplete()
  //        context.stop(self)
  //      }
  //      emptyQueue
  //    } else if (totalDemand == 0) {
  //      q
  //    } else {
  //      val (toDeliver, remaining) = q.splitAt(totalDemand.toInt)
  //      toDeliver.foreach(onNext(_))
  //      remaining
  //    }
  //  }

}
