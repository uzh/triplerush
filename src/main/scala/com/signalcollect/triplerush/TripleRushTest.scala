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
import akka.actor.actorRef2Scala
import akka.cluster.sharding.ClusterSharding
import akka.pattern.ask
import akka.util.Timeout
import com.signalcollect.triplerush.index.Index

object TripleRushTest extends App {
  val cluster = ClusterCreator.create(2)
  cluster.foreach(Index.registerWithSystem(_))

  val indexRegions = cluster.map { system =>
    ClusterSharding(system).shardRegion(Index.shardName)
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
