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
import scala.reflect.ClassTag

import akka.actor.{ Actor, ActorLogging }
import akka.event.LoggingReceive
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request

import collection.JavaConversions._

object Streamer {

  case object Completed

}

// TODO: Define timeout and terminate when it is reached.
// TODO: Max buffer size?
// TODO: Use back pressure before the buffer becomes full.
abstract class Streamer[I: ClassTag]() extends ActorPublisher[I] with ActorLogging {

  // Point this at an object to save the reference.
  def emptyQueue = ???

  def receive: Actor.Receive = buffering(emptyQueue, completed = false)

  /**
   * Buffers the queued items and stores if the stream has been completed.
   */
  protected def buffering(buffer: Queue[I], completed: Boolean): Actor.Receive = LoggingReceive {
    case item: I =>
      // Skip buffering if possible.
      if (buffer.isEmpty && totalDemand > 0) {
        onNext(item)
        context.become(buffering(emptyQueue, completed))
      } else {
        val updatedQueue = buffer.enqueue(item)
        val remainingQueue = deliverFromQueue(updatedQueue, completed)
        context.become(buffering(remainingQueue, completed))
      }
    case items: Array[I] =>
      // Skip buffering if possible.
      if (buffer.isEmpty && totalDemand > items.length) {
        items.foreach(onNext)
        context.become(buffering(emptyQueue, completed))
      } else {
        val updatedQueue: Queue[I] = ??? //buffer.enqueue(iterableItems)
        val remainingQueue = deliverFromQueue(updatedQueue, completed)
        context.become(buffering(remainingQueue, completed))
      }
    case Streamer.Completed =>
      val remaining = deliverFromQueue(buffer, completed)
      context.become(buffering(remaining, completed))
    case Request(count) =>
      val remaining = deliverFromQueue(buffer, completed)
      context.become(buffering(remaining, completed))
  }

  /**
   * Delivers from `queued' whatever it can, then returns a queue with the remaining items.
   * Completes the out bound stream if all results were delivered the in bound stream has completed.
   */
  protected def deliverFromQueue(queued: Queue[I], completed: Boolean): Queue[I] = {
    if (queued == emptyQueue) {
      if (completed) {
        onCompleteThenStop()
      }
      emptyQueue
    } else {
      // Queue contains elements.
      if (totalDemand == 0) {
        queued
      } else if (totalDemand >= queued.size) {
        queued.foreach(onNext)
        if (completed) {
          onCompleteThenStop()
        }
        emptyQueue
      } else {
        // we can be sure that totalDemand < queued.size,
        // which means it can safely be converted to an Int.
        val (toDeliver, remaining) = queued.splitAt(totalDemand.toInt)
        toDeliver.foreach(onNext)
        remaining
      }
    }
  }

}
