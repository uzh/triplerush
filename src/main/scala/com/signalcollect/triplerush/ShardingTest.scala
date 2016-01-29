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

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import com.typesafe.config.ConfigFactory
import akka.actor.{ Actor, ActorSystem, Props }
import akka.cluster.ddata.{ DistributedData, ORSet, ORSetKey }
import akka.cluster.ddata.Replicator.{ Subscribe, _ }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.pattern.ask
import akka.util.Timeout
import akka.cluster.ddata.Replicator
import akka.cluster.Cluster
import akka.actor.ActorLogging
import akka.cluster.ddata.GSetKey
import akka.cluster.ddata.GSet

object Index {

  val shardName = "index"
  def props: Props = Props(new Index)

  case class IndexContent(childIds: Set[String]) {
    def +(child: String) = copy(childIds + child)
  }

  sealed trait Command {
    def indexId: String
  }
  case class CreateIndex(indexId: String, content: IndexContent) extends Command
  case class AddChildId(indexId: String, childId: String) extends Command
  case class GetChildIds(indexId: String) extends Command

  sealed trait Event
  case class IndexAdded(content: IndexContent) extends Event
  case class ChildAdded(child: String) extends Event

  val idExtractor: ShardRegion.ExtractEntityId = {
    case command: Command => (command.indexId.toString, command)
  }

  val shardResolver: ShardRegion.ExtractShardId = {
    case command: Command => ((command.indexId.hashCode & Int.MaxValue) % 100).toString
  }

  private case class State(childIds: IndexContent) {

    def updated(event: Event): State = event match {
      case IndexAdded(c) => copy(childIds = c)
      case ChildAdded(c) => copy(childIds = childIds + c)
    }
  }
}

class Index extends Actor with ActorLogging {

  var content: Index.IndexContent = Index.IndexContent(Set.empty)

  def receive = {
    case g @ Index.GetChildIds(_) =>
      log.info(s"Index actor running on system ${context.self} received message $g, content is ${content}")
      sender() ! content
    case a @ Index.CreateIndex(indexId, indexContent) =>
      content = indexContent
      println(s"Index actor running on system ${context.self} received message $a, content is now ${content}")
    case a @ Index.AddChildId(indexId, childId) =>
      content = Index.IndexContent(content.childIds + childId)
      log.info(s"Index actor running on system ${context.self} received message $a, content is now ${content}")
    case other =>
      log.info(s"Index actor running on system ${context.self} received message $other")
  }

}

object ClusterCreator {
  def create(numberOfNodes: Int): Seq[ActorSystem] = {
    for {
      nodeId <- 0 until numberOfNodes
      portConfig = if (nodeId == 0) ConfigFactory.parseString("akka.remote.netty.tcp.port = 2551") else ConfigFactory.empty
    } yield ActorSystem(
      "ClusterSystem",
      portConfig.withFallback(ConfigFactory.load().getConfig("triplerush")))
  }
}

object ShardingTest extends App {

  val cluster = ClusterCreator.create(2)

  val shardActors = cluster.map { actorSystem =>
    ClusterSharding(actorSystem).start(
      typeName = Index.shardName,
      entityProps = Index.props,
      settings = ClusterShardingSettings(actorSystem),
      extractEntityId = Index.idExtractor,
      extractShardId = Index.shardResolver)
  }

  val indexRegions = cluster.map { actorSystem =>
    ClusterSharding(actorSystem).shardRegion(Index.shardName)
  }

  val indexId = UUID.randomUUID().toString
  indexRegions.head ! Index.CreateIndex(indexId, Index.IndexContent(Set("a", "b")))

  implicit val timeout = new Timeout(30.seconds)

  Thread.sleep(5000)
  
  val childIdsFuture = indexRegions.last ? Index.GetChildIds(indexId)

  childIdsFuture.onComplete { result =>
    println(s"result = $result")
  }

}
