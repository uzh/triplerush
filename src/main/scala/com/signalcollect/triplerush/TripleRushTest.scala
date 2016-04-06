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

import scala.Vector
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Random

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }

object TripleRushTest extends App {

  val config = ConfigFactory.load()
  val numberOfNodes = config.getInt("triplerush.number-of-nodes")
  val cluster = ClusterCreator.create(numberOfNodes)
  val tr = TripleRush(Random.shuffle(cluster).head)

  val a = 1
  val customerA = 2
  val customer = 3
  val orderA = 4
  val order = 5
  val customerB = 6
  val orderB = 7
  val ordered = 8
  val price = 9
  val oneThousand = 10
  val oneHundred = 11

  val customerVar = -1
  val orderVar = -2

  val triplesSource = Source(List(
    TriplePattern(customerA, a, customer),
    TriplePattern(customerB, a, customer),
    TriplePattern(orderA, a, order),
    TriplePattern(orderA, price, oneHundred),
    TriplePattern(orderB, price, oneThousand),
    TriplePattern(orderB, a, order),
    TriplePattern(customerA, ordered, orderA),
    TriplePattern(customerB, ordered, orderB)))
  val doneLoading = tr.addTriplePatterns(triplesSource)
  Await.ready(doneLoading, 30.seconds)

  val results = tr.query(Vector(
    TriplePattern(customerVar, a, customer),
    TriplePattern(customerVar, ordered, orderVar),
    TriplePattern(orderVar, price, oneThousand)))
  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()
  val printingSink = Sink.foreach[Bindings](b => println(s"BINDINGS: ${b.asString}"))
  println("okay, waiting for results now")
  val printing = results.runWith(printingSink)

  Await.ready(printing, 30.seconds)
  tr.close()

  cluster.foreach(_.terminate())

}
