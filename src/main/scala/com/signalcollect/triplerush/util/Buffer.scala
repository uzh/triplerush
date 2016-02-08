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
import akka.actor.{ Actor, ActorLogging, ActorRef }
import akka.actor.util.FlushWhenIdle
import akka.actor.util.Flush

final class Buffer[I: ClassTag](
  destination: ActorRef,
  maxBufferSize: Int = 10000)
    extends Actor with FlushWhenIdle with ActorLogging {

  private[this] val buffer = new ReusableBuffer[I](maxBufferSize)

  def receive: Receive = {
    case item: I =>
      buffer.add(item)
      if (buffer.isFull) {
        flush()
      }
    case Flush =>
      flush()
  }

  def flush(): Unit = {
    destination ! buffer.copy()
    buffer.clear()
  }

}
