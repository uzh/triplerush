/*
 *  @author Philip Stutz
 *  @author Jahangir Mohammed
 *
 *  Copyright 2015 iHealth Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.signalcollect.triplerush.loading

import com.signalcollect.triplerush._

object TriplesLoading extends App {
  val start = System.currentTimeMillis
//  val groupSize = 8
//  val batchSize = 10000
//  var triplesSoFar = 0
  val tr = TripleRush(fastStart = true)
//  val i = TripleIterator(args(0)).take(10000000).grouped(batchSize).grouped(groupSize).foreach { sequenceOfSequences =>
//    sequenceOfSequences.map { s => tr.loadFromIterator(s.iterator) }
//    val batchStart = System.currentTimeMillis
//    println(s"waiting for batch of ${groupSize * batchSize}")
//    tr.awaitIdle
//    triplesSoFar += groupSize * batchSize
//    println(s"batch finished, took ${((System.currentTimeMillis - batchStart) / 100.0).round / 10.0} seconds")
//    println(s"total triples so far: $triplesSoFar")
//    println(s"dictionary stats = ${tr.dictionary}")
//    println(s"time so far: ${((System.currentTimeMillis - start) / 100.0).round / 10.0} seconds")
//    println(s"time per million triples so far: ${((System.currentTimeMillis - start) / 100.0 / (triplesSoFar / 1000000)).round / 10.0} seconds")
//  }
  tr.loadFromFile(args(0))
  tr.awaitIdle()
  println(tr.dictionary)
  tr.shutdown

}
