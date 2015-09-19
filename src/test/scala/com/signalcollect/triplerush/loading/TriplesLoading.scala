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
import com.signalcollect.triplerush.dictionary.HashDictionary
import com.signalcollect.GraphBuilder
import com.signalcollect.triplerush.dictionary.RdfDictionary

object MockDictionary extends RdfDictionary {
  def contains(s: String): Boolean = false
  def apply(s: String): Int = s.hashCode & Int.MaxValue
  def get(s: String): Option[Int] = None
  def contains(i: Int): Boolean = false
  def apply(i: Int): String = "hello!"
  def get(i: Int): Option[String] = Some(apply(i))
  def clear(): Unit = Unit
  def close(): Unit = Unit
}

object TriplesLoading extends App {
  val start = System.currentTimeMillis
  var triplesSoFar = 0
  val batchSize = 10000
  val prefixes = List("\"\"\"\"\"\"\"\"\"\"", "http://www.ihtech.com/aagaami#", "http://www.ihtech.com/aagaami#hasClaim", "http://www.ihtech.com/aagaami#claimId", "http://www.ihtech.com/aagaami#hasClaimLine", "http://www.ihtech.com/aagaami#providerId", "http://www.ihtech.com/aagaami#lineNo", "http://www.ihtech.com/aagaami#dosFrom", "http://www.ihtech.com/aagaami#dosTo", "http://www.ihtech.com/aagaami#units", "http://www.ihtech.com/aagaami#billType", "http://www.ihtech.com/aagaami#primaryInsuredId", "http://www.ihtech.com/aagaami#billedCptCode", "http://www.ihtech.com/aagaami#zipCode", "http://www.ihtech.com/aagaami#hasModifier")
  //  val dictionary = new HashDictionary(prefixes)
  val dictionary = MockDictionary
  val tr = TripleRush(
    fastStart = true,
    dictionary = dictionary,
    graphBuilder = new GraphBuilder[Long, Any]())
  val i = TripleIterator(args(0))
  i.grouped(batchSize).foreach { triples =>
    val batchStart = System.currentTimeMillis
    tr.addTriples(triples.iterator, true)
    triplesSoFar += batchSize
    println(s"batch finished, took ${((System.currentTimeMillis - batchStart) / 100.0).round / 10.0} seconds")
    println(s"total triples so far: $triplesSoFar")
    println(s"dictionary stats = ${tr.dictionary}")
    println(s"time so far: ${((System.currentTimeMillis - start) / 100.0).round / 10.0} seconds")
    println(s"time per million triples so far: ${((System.currentTimeMillis - start) / 100.0 / (triplesSoFar / 1000000.0)).round / 10.0} seconds")
  }
  println(tr.dictionary)
  tr.shutdown

}
