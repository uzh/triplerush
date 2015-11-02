/*
 *  @author Philip Stutz
 *  @author Bharath Kumar
 *
 *  Copyright 2015 iHealth Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.triplerush

import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.reflect.runtime.universe
import scala.util.{ Failure, Success, Try }
import org.scalatest.fixture.NoArg
import com.signalcollect.GraphBuilder
import com.signalcollect.configuration.Akka
import com.signalcollect.triplerush.sparql.TripleRushGraph
import com.typesafe.config.{ Config, ConfigFactory }
import akka.actor.ActorSystem
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object TestStore {
  
  private[this] val uniquePrefixTracker = new AtomicInteger(0)

  private[this] val uniqueNameTracker = new AtomicInteger(0)

  def nextUniquePrefix = uniquePrefixTracker.incrementAndGet.toString

  def nextUniqueName = uniqueNameTracker.incrementAndGet.toString

  def instantiateUniqueStore(): TripleRush = {
    val uniquePrefix = nextUniquePrefix
    val graphBuilder = instantiateUniqueGraphBuilder
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

  def instantiateUniqueActorSystem(): ActorSystem = {
    val defaultGraphConfig = GraphBuilder.config
    val defaultAkkaConfig = Akka.config(
      serializeMessages = None,
      loggingLevel = None,
      kryoRegistrations = defaultGraphConfig.kryoRegistrations,
      kryoInitializer = None)
    val actorSystemName = nextUniqueName
    val customAkkaConfig = customClusterConfig(actorSystemName = actorSystemName, seedPort = freePort)
      .withFallback(defaultAkkaConfig)
    ActorSystem(actorSystemName, customAkkaConfig)
  }

  def instantiateUniqueGraphBuilder(): GraphBuilder[Long, Any] = {
    new GraphBuilder[Long, Any]()
      .withActorNamePrefix(nextUniquePrefix)
      .withActorSystem(instantiateUniqueActorSystem())
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

class TestStore(val tr: TripleRush) extends NoArg {

  lazy implicit val graph = TripleRushGraph(tr)
  lazy implicit val model = graph.getModel
  lazy implicit val system = tr.graph.system

  def this() = this(TestStore.instantiateUniqueStore())

  def shutdown(): Unit = {
    model.close()
    graph.close()
    tr.shutdown()
    Await.result(tr.graph.system.terminate(), Duration.Inf)
  }

  override def apply(): Unit = {
    try {
      super.apply()
    } finally {
      shutdown()
    }
  }

}
