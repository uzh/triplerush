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

import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe
import scala.util.{ Failure, Success, Try }

import org.scalatest.fixture.NoArg

import com.signalcollect.GraphBuilder
import com.signalcollect.configuration.Akka
import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.ActorSystem

object TestStore {

  val numberOfCoresInTests = 2

  def instantiateUniqueStore(): TripleRush = {
    val graphBuilder = instantiateUniqueGraphBuilder()
    TripleRush(graphBuilder = graphBuilder)
  }

  def customClusterConfig(actorSystemName: String, seedPort: Int, seedIp: String = "127.0.0.1"): Config = {
    ConfigFactory.parseString(
      s"""|akka.clustering.name=$actorSystemName
          |akka.clustering.seed-ip=$seedIp
          |akka.clustering.seed-port=$seedPort
          |akka.remote.netty.tcp.port=$seedPort
          |akka.cluster.seed-nodes=["akka.tcp://"${actorSystemName}"@"${seedIp}":"${seedPort}]"""
        .stripMargin)
  }

  def instantiateUniqueActorSystem(cores: Int = numberOfCoresInTests): ActorSystem = {
    val defaultGraphConfig = GraphBuilder.config
    val defaultAkkaConfig = Akka.config(
      serializeMessages = None,
      loggingLevel = None,
      kryoRegistrations = defaultGraphConfig.kryoRegistrations,
      kryoInitializer = None,
      numberOfCores = cores)
    val actorSystemName = "TripleRushTestSystem"
    val customAkkaConfig = customClusterConfig(actorSystemName = actorSystemName, seedPort = freePort)
      .withFallback(defaultAkkaConfig)
    ActorSystem(actorSystemName, customAkkaConfig)
  }

  def instantiateUniqueGraphBuilder(cores: Int = numberOfCoresInTests): GraphBuilder[Long, Any] = {
    new GraphBuilder[Long, Any]()
      .withActorSystem(instantiateUniqueActorSystem(cores))
  }

  private[this] val portUsageTracker = new AtomicInteger(2500)

  def freePort: Int = {
    @tailrec def attemptToBindAPort(failuresSoFar: Int): Int = {
      val checkedPort = portUsageTracker.incrementAndGet
      val socketTry = Try(new ServerSocket(checkedPort))
      socketTry match {
        case Success(s) =>
          s.close()
          checkedPort
        case Failure(f) =>
          if (failuresSoFar > 10) {
            throw f
          } else {
            attemptToBindAPort(failuresSoFar + 1)
          }
      }
    }
    attemptToBindAPort(0)
  }

}

class TestStore(storeInitializer: => TripleRush) extends NoArg {

  lazy val tr: TripleRush = storeInitializer
  lazy implicit val model = tr.getModel
  lazy implicit val system = tr.graph.system

  def this() = this(TestStore.instantiateUniqueStore)

  def close(): Unit = {
    model.close()
    tr.close()
    Await.result(tr.graph.system.terminate(), Duration.Inf)
  }

  override def apply(): Unit = {
    try {
      super.apply()
    } finally {
      close()
    }
  }

}
