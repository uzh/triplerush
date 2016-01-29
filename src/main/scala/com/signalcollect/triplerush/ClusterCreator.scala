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

package com.signalcollect.triplerush

import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorIdentity, ActorPath, ActorSystem, Identify, Props }
import akka.pattern.ask
import akka.persistence.journal.leveldb.{ SharedLeveldbJournal, SharedLeveldbStore }
import akka.util.Timeout

object ClusterCreator {

  def create(numberOfNodes: Int): Seq[ActorSystem] = {
    val nodeZeroPort = 2551
    val systems = for {
      nodeId <- 0 until numberOfNodes
      portConfig = if (nodeId == 0) ConfigFactory.parseString(s"akka.remote.netty.tcp.port = $nodeZeroPort") else ConfigFactory.empty
    } yield ActorSystem(
      "ClusterSystem",
      portConfig.withFallback(ConfigFactory.load().getConfig("triplerush")))
    systems.zipWithIndex.foreach {
      case (system, index) => startupSharedJournal(system, startStore = index == 0, path =
        ActorPath.fromString(s"akka.tcp://ClusterSystem@127.0.0.1:$nodeZeroPort/user/store"))
    }
    systems
  }

  def startupSharedJournal(system: ActorSystem, startStore: Boolean, path: ActorPath): Unit = {
    if (startStore) {
      system.actorOf(Props[SharedLeveldbStore], "store")
    }
    implicit val timeout = Timeout(15.seconds)
    import system.dispatcher
    val f = (system.actorSelection(path) ? Identify(None))
    f.onSuccess {
      case ActorIdentity(_, Some(ref)) => SharedLeveldbJournal.setStore(ref, system)
      case _ =>
        system.log.error("Shared journal not started at {}", path)
        system.terminate()
    }
    f.onFailure {
      case _ =>
        system.log.error("Lookup of shared journal at {} timed out", path)
        system.terminate()
    }
  }

}
