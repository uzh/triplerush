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
import com.signalcollect.triplerush.TriplePattern
import akka.actor.Actor
import akka.actor.ActorRef
import com.signalcollect.triplerush.Shard
import com.signalcollect.triplerush.AdjacencySets
import com.signalcollect.triplerush.SimpleAdjacencySets
import com.signalcollect.triplerush.query.QueryExecutionHandler
import com.signalcollect.triplerush.Bindings

case class OutOfTicketsException(msg: String) extends Exception(msg)

object Index extends Shard {

  def apply(): Actor = new Index

  case class AddEdge(indexId: Int, edge: Edge)
  case class GetAdjacencySets(indexId: Int)
  case object EdgeAdded

  val idExtractor: ShardRegion.ExtractEntityId = {
    case AddEdge(indexId, edge) =>
      (indexId.toString, edge)
    case GetAdjacencySets(indexId) =>
      (indexId.toString, Unit)
    case queryParticle: QueryParticle =>
      val indexId = queryParticle.indexRoutingAddress
      (indexId.toString, queryParticle)
  }

  // TODO: Use locality-optimized sharding function for TR index.
  def indexIdToShardId(indexId: Long): String = {
    ((indexId.hashCode & Int.MaxValue) % 100).toString
  }

  val shardResolver: ShardRegion.ExtractShardId = {
    case AddEdge(indexId, _) =>
      indexIdToShardId(indexId)
    case GetAdjacencySets(indexId) =>
      indexIdToShardId(indexId)
    case queryParticle: QueryParticle =>
      val indexId = queryParticle.indexRoutingAddress
      indexIdToShardId(indexId)
  }

}

final class Index extends Actor with ActorLogging {

  def indexId: Int = self.path.name.toInt
  override def toString(): String = {
    s"Vertex(indexId=${indexId.toString}, adjacencySets=$adjacencySets)"
  }

  val adjacencySets: AdjacencySets = new SimpleAdjacencySets

  def receive = {
    case Unit =>
      sender() ! adjacencySets
    case e: Edge =>
      adjacencySets.addEdge(e)
      sender() ! Index.EdgeAdded
    case query: QueryParticle =>
      val queryVertex = QueryExecutionHandler.shard(context.system)
      val indexVertex = Index.shard(context.system)
      val incomingPatterns: Iterable[TriplePattern] = {
        val o = indexId
        for {
          p <- adjacencySets.incoming.keys
          s <- adjacencySets.incoming(p)
        } yield TriplePattern(s, p, o)
      }
      val outgoingPatterns: Iterable[TriplePattern] = {
        val s = indexId
        for {
          p <- adjacencySets.incoming.keys
          o <- adjacencySets.incoming(p)
        } yield TriplePattern(s, p, o)
      }
      val allTriples = incomingPatterns ++ outgoingPatterns
      println(s"allTriples=$allTriples will bind $query")
      val allBound = allTriples.flatMap(query.bind(_))
      val numberOfPartcilesToRoute = allBound.size
      if (numberOfPartcilesToRoute == 0) {
        queryVertex ! QueryExecutionHandler.Tickets(query.id, query.tickets)
      } else {
        val evenlySplitTickets = query.tickets / allBound.size
        val extraTickets = query.tickets % allBound.size
        val head = allBound.head
        def handleQuery(q: QueryParticle): Unit = {
          if (q.isResult) {
            queryVertex ! QueryExecutionHandler.BindingsForQuery(q.id, Bindings(q.bindings))
            queryVertex ! QueryExecutionHandler.Tickets(q.id, q.tickets)
          } else {
            indexVertex ! q
          }
        }
        handleQuery(head.copy(tickets = evenlySplitTickets + extraTickets))
        allBound.tail.foreach { boundQuery =>
          handleQuery(boundQuery.copy(tickets = evenlySplitTickets))
        }
      }
  }

}
