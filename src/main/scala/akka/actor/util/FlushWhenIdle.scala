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

package akka.actor.util

import akka.actor.{ Actor, ActorLogging }
import akka.actor.actorRef2Scala

case object FlushIfIdle

/**
 * Trait that can be used for actors that want to be able delay a `flush` action
 * whilst there is more work to do, but immediately perform that action once they are
 * idle.
 *
 * Needs to be placed in package `akka` for access to `aroundReceive`.
 */
trait FlushWhenIdle extends Actor {

  protected def flush(): Unit

  private[this] var idle = true
  private[this] var awaitingFlushIfIdle = false

  protected[akka] override def aroundReceive(receive: Receive, msg: Any): Unit = msg match {
    case FlushIfIdle =>
      awaitingFlushIfIdle = false
      if (idle) {
        flush()
      } else {
        startIdleDetection()
      }
    case other: Any =>
      super.aroundReceive(receive, msg)
      idle = false
      if (!awaitingFlushIfIdle) {
        startIdleDetection()
      }
  }

  @inline def startIdleDetection(): Unit = {
    idle = true
    awaitingFlushIfIdle = true
    self ! FlushIfIdle
  }

}
