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

package com.signalcollect.triplerush.cluster

import scala.concurrent.duration.DurationInt
import org.scalatest.Finders
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import com.signalcollect.GraphBuilder
import com.signalcollect.nodeprovisioning.cluster.ClusterNodeProvisionerActor
import com.signalcollect.nodeprovisioning.cluster.RetrieveNodeActors
import com.signalcollect.triplerush.TestStore
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.mapper.DistributedTripleMapperFactory
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import com.signalcollect.triplerush.sparql.Sparql
import collection.JavaConversions._

class ClusterSpec extends FlatSpec with Matchers with ScalaFutures {

  def instantiateMasterAndSlaveActorSystems(cores: Int = TestStore.numberOfCoresInTests): (ActorSystem, ActorSystem) = {
    val masterPort = TestStore.freePort
    val masterSystem = TestStore.instantiateUniqueActorSystem(port = masterPort, seedPortOption = Some(masterPort), actorSystemName = "ClusterSystem")
    val slaveSystem = TestStore.instantiateUniqueActorSystem(seedPortOption = Some(masterPort), actorSystemName = "ClusterSystem")
    (masterSystem, slaveSystem)
  }

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(1000, Millis)))

  "DistributedTripleRush" should "load a triple and execute a simple SPARQL query" in {
    val numberOfNodes = 2
    implicit val timeout = Timeout(30.seconds)
    val (master, slave) = instantiateMasterAndSlaveActorSystems()
    try {
      val mapperFactory = DistributedTripleMapperFactory
      val masterActor = master.actorOf(Props(classOf[ClusterNodeProvisionerActor], 1000,
        "", numberOfNodes), "ClusterMasterBootstrap")
      val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
      whenReady(nodeActorsFuture) { nodeActors =>
        assert(nodeActors.length == numberOfNodes)
        val graphBuilder = new GraphBuilder[Long, Any]().
          withActorSystem(master).
          withPreallocatedNodes(nodeActors)
        val tr = TripleRush(
          graphBuilder = graphBuilder,
          tripleMapperFactory = Some(mapperFactory))
        try {
          master.log.info(s"TripleRush has been initialized.")
          implicit val model = tr.getModel
          tr.addStringTriple("http://s", "http://p", "http://o")
          val result = Sparql("SELECT ?s { ?s <http://p> <http://o> }")
          assert(result.hasNext, "There was no query result, but there should be one.")
          val next = result.next
          assert(!result.hasNext, "There should be exactly one result, but found more.")
          assert(next.get("s").toString == "http://s")
        } finally {
          tr.close
        }
      }
    } finally {
      slave.terminate
      master.terminate
    }
  }

}
