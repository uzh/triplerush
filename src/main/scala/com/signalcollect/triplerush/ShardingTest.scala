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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import com.typesafe.config.ConfigFactory
import akka.actor.{ ActorLogging, ActorSystem, Props, actorRef2Scala }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.pattern.ask
import akka.persistence.PersistentActor
import akka.util.Timeout
import akka.actor.ActorPath
import akka.persistence.journal.leveldb.SharedLeveldbStore
import akka.actor.Identify
import akka.actor.ActorIdentity
import akka.persistence.journal.leveldb.SharedLeveldbJournal

trait IntSet {
  def add(i: Int): IntSet
  def foreach(f: Int => Unit): Unit
}

case class SimpleIntSet(items: Set[Int] = Set.empty[Int]) extends IntSet {
  def add(i: Int) = SimpleIntSet(items + i)
  def foreach(f: Int => Unit): Unit = items.foreach(f)
  override def toString = items.toString
}

object Index {

  val shardName = "index"
  val props: Props = Props(new Index())

  sealed trait Command {
    def indexId: String
  }
  case class AddChildId(indexId: String, childId: Int) extends Command
  case class GetChildIds(indexId: String) extends Command

  val idExtractor: ShardRegion.ExtractEntityId = {
    case AddChildId(indexId, childId) => (indexId.toString, childId)
    case GetChildIds(indexId)         => (indexId.toString, Unit)
  }

  val shardResolver: ShardRegion.ExtractShardId = {
    case command: Command => ((command.indexId.hashCode & Int.MaxValue) % 100).toString
  }

}

class Index extends PersistentActor with ActorLogging {
  import Index._

  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  var childIds: IntSet = new SimpleIntSet

  def receiveCommand = {
    case Unit =>
      log.info(s"Index actor running on system ${context.self} received message GetChildIds, content is ${childIds}")
      sender() ! childIds
    case childId: Int =>
      persist(childId) { id =>
        childIds = childIds.add(id)
        log.info(s"Index actor running on system ${context.self} received message $childId, content is now ${childIds}")
      }
    case other =>
      log.info(s"Index actor running on system ${context.self} received message $other")
  }

  override def receiveRecover: Receive = {
    case childId: Int => childIds.add(childId)
  }

}

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
  indexRegions.head ! Index.AddChildId(indexId, 1)
  indexRegions.last ! Index.AddChildId(indexId, 2)

  implicit val timeout = new Timeout(30.seconds)

  Thread.sleep(5000)

  val childIdsFuture = indexRegions.last ? Index.GetChildIds(indexId)

  childIdsFuture.onComplete { result =>
    println(s"result = $result")
  }

}
