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

import com.signalcollect.triplerush.index.Index
import com.signalcollect.triplerush.query.QueryExecutionHandler
import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem

object ClusterCreator {

  def create(numberOfNodes: Int): Seq[ActorSystem] = {
    val nodeZeroPort = 2551
    val systems = for {
      nodeId <- 0 until numberOfNodes
      portConfig = if (nodeId == 0) ConfigFactory.parseString(s"akka.remote.netty.tcp.port = $nodeZeroPort") else ConfigFactory.empty
    } yield ActorSystem(
      "ClusterSystem",
      portConfig.withFallback(ConfigFactory.load().getConfig("triplerush")))
    systems.foreach { system =>
      Index.registerWithSystem(system)
      QueryExecutionHandler.registerWithSystem(system)
    }
    systems
  }

}
