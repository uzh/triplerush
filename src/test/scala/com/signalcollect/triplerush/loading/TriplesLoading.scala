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

package com.signalcollect.triplerush.loading

import scala.collection.JavaConversions.asScalaIterator
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.io.StdIn

import com.signalcollect.triplerush.Lubm
import com.signalcollect.triplerush.TestStore
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.sparql.Sparql

import akka.actor.ActorSystem

object TriplesLoading extends App {

  println("Press any key to run.")
  StdIn.readLine()
  execute()

  def execute(): Unit = {
    val (tr, slaves): (TripleRush, Seq[ActorSystem]) = TestStore.instantiateDistributedStore(TestStore.freePort, 10)
    implicit val model = tr.getModel
    implicit val system = tr.graph.system

    try {
      Lubm.load(tr, 14)
      for { query <- Lubm.sparqlQueries } {
        val numberOfResults = Sparql(query).size
      }
    } finally {
      model.close()
      tr.close()
      Await.ready(Future.sequence(slaves.map(_.terminate())), 30.seconds)
      Await.ready(tr.graph.system.terminate(), Duration.Inf)
    }
  }

}
