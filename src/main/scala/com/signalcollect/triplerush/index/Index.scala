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
import akka.actor.actorRef2Scala
import com.signalcollect.triplerush.IntSet
import com.signalcollect.triplerush.SimpleIntSet
import com.signalcollect.triplerush.EfficientIndexPattern._
import com.signalcollect.triplerush.TriplePattern
import com.signalcollect.triplerush.query.QueryParticle
import com.signalcollect.triplerush.query.QueryParticle._
import com.signalcollect.triplerush.query.ParticleDebug
import akka.actor.Actor
import akka.actor.ActorRef
import com.signalcollect.triplerush.Shard
import com.signalcollect.triplerush.query.QueryExecutionHandler

case class OutOfTicketsException(msg: String) extends Exception(msg)

object Index extends Shard {

  def apply(): Actor = new Index

  case class AddChildId(indexId: Long, childId: Int)
  case class GetChildIds(indexId: Long)
  case object ChildIdAdded

  val idExtractor: ShardRegion.ExtractEntityId = {
    case AddChildId(indexId, childId) =>
      (indexId.toString, childId)
    case GetChildIds(indexId) =>
      (indexId.toString, Unit)
    case queryParticleArray: Array[Int] =>
      val particle = new QueryParticle(queryParticleArray)
      val indexId = particle.routingAddress
      (indexId.toString, queryParticleArray)
  }

  // TODO: Use locality-optimized sharding function for TR index.
  def indexIdToShardId(indexId: Long): String = {
    ((indexId.hashCode & Int.MaxValue) % 100).toString
  }

  val shardResolver: ShardRegion.ExtractShardId = {
    case AddChildId(indexId, childId) =>
      indexIdToShardId(indexId)
    case GetChildIds(indexId) =>
      indexIdToShardId(indexId)
    case queryParticleArray: Array[Int] =>
      val particle = new QueryParticle(queryParticleArray)
      val indexId = particle.routingAddress
      indexIdToShardId(indexId)
  }

}

final class Index extends Actor with ActorLogging {

  def indexId: String = self.path.name
  override def toString(): String = {
    indexId.toLong.toTriplePattern.toString
  }

  var childIds: IntSet = new SimpleIntSet

  def receive = {
    case Unit =>
      sender() ! childIds
    case childId: Int =>
      childIds = childIds.add(childId)
      sender() ! Index.ChildIdAdded
    case queryParticle: Array[Int] =>
      val indexIdAsLong = indexId.toLong
      if (queryParticle.lastPattern.isFullyBound) {
        // TODO: Ensure this always matches the index routing or is dynamically retrieved.
        val toCheck = queryParticle.lastPattern.o
        if (childIds.contains(toCheck)) {
          if (queryParticle.numberOfPatterns == 1) {
            // It's a result.
            val query = QueryExecutionHandler.shard(context.system)
            query ! QueryExecutionHandler.BindingsForQuery(queryParticle.queryId, queryParticle.bindings)
            query ! QueryExecutionHandler.Tickets(queryParticle.queryId, queryParticle.tickets)
          } else {
            // Route it onwards.
            val indexShard = Index.shard(context.system)
            val updated = queryParticle.copyWithoutLastPattern
            indexShard ! updated
          }
        } else {
          // Failed to pass existence check, return tickets.
          val query = QueryExecutionHandler.shard(context.system)
          query ! QueryExecutionHandler.Tickets(queryParticle.queryId, queryParticle.tickets)
        }
      } else {
        IndexType(indexIdAsLong) match {
          case f: Forwarding =>
            Forward.forwardQuery(context.system, indexId.toLong, queryParticle, childIds.size, childIds)
          case b: Binding =>
            Bind.bindQuery(context.system, indexId.toLong, queryParticle, childIds.size, childIds)
        }
      }
  }

}
