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

package com.signalcollect.triplerush.index

import akka.actor.{ ActorLogging, ActorSystem, Props }
import akka.cluster.sharding.{ ClusterSharding, ClusterShardingSettings, ShardRegion }
import akka.persistence.PersistentActor
import akka.actor.actorRef2Scala
import com.signalcollect.triplerush.IntSet
import com.signalcollect.triplerush.SimpleIntSet
import com.signalcollect.triplerush.EfficientIndexPattern._

object Index {

  def registerWithSystem(system: ActorSystem): Unit = {
    ClusterSharding(system).start(
      typeName = shardName,
      entityProps = props,
      settings = ClusterShardingSettings(system),
      extractEntityId = Index.idExtractor,
      extractShardId = Index.shardResolver)
  }

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

  override def persistenceId: String = self.path.name
  
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