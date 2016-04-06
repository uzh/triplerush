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

package com.signalcollect.triplerush.util

import scala.reflect.ClassTag

import akka.actor.Actor
import akka.actor.util.{ Flush, FlushWhenIdle }
import akka.contrib.pattern.ReceivePipeline
import akka.contrib.pattern.ReceivePipeline.Inner

object Streamer {

  case object Completed

  case object DeliverFromQueue

  class QueueFullException(is: Any) extends Exception(s"Could not add items $is to the queue.")

}

// TODO: Define timeout and terminate when it is reached.
// TODO: Max buffer size?
// TODO: Use back pressure before the buffer becomes full.
abstract class Streamer[I: ClassTag]() extends Actor with FlushWhenIdle with ReceivePipeline {

  def bufferSize: Int

  def canDeliver: Boolean

  protected val queue = new FifoQueue[I](bufferSize)

  pipelineInner {
    case i: I =>
      val wasPut = queue.put(i)
      assert(wasPut)
      if (queue.isFull) {
        Inner(Streamer.DeliverFromQueue)
      } else {
        ReceivePipeline.HandledCompletely
      }
    case is: Array[I] =>
      println(s"$self received ${is.size} results")
      if (queue.freeCapacity >= is.length) {
        val wasPut = queue.batchPut(is)
        assert(wasPut)
        if (queue.isFull) {
          Inner(Streamer.DeliverFromQueue)
        } else {
          ReceivePipeline.HandledCompletely
        }
      } else {
        throw new Streamer.QueueFullException(is)
      }
    case Flush =>
      if (!queue.isEmpty && canDeliver) {
        Inner(Streamer.DeliverFromQueue)
      } else {
        ReceivePipeline.HandledCompletely
      }
  }

}
