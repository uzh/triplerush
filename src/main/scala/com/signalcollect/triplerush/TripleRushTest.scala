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
import scala.concurrent.Await
import scala.concurrent.Future
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializerSettings
import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import akka.stream.Supervision
import scala.util.Random
import akka.stream.impl.fusing.ActorGraphInterpreter
import com.typesafe.config.ConfigFactory
import akka.stream.scaladsl.Source
import com.signalcollect.triplerush.query.VariableEncoding

object TripleRushTest extends App {

  val config = ConfigFactory.load()
  val numberOfNodes = config.getInt("triplerush.number-of-nodes")
  val cluster = ClusterCreator.create(numberOfNodes)
  val tr = TripleRush(Random.shuffle(cluster).head)

  val triplesSource = Source(List(
    TriplePattern(1, 2, 3),
    TriplePattern(1, 2, 4),
    TriplePattern(1, 5, 3),
    TriplePattern(6, 2, 3)))
  val doneLoading = tr.addTriplePatterns(triplesSource)
  Await.ready(doneLoading, 30.seconds)

  val results = tr.query(Vector(TriplePattern(-1, 2, 3)))
  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()
  val printingSink = Sink.foreach[Bindings](b => println(s"BINDINGS: ${b.asString}"))
  println("okay, waiting for results now")
  val printing = results.runWith(printingSink)

  Await.ready(printing, 30.seconds)
  tr.close()

  cluster.foreach(_.terminate())

}
