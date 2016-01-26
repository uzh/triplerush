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

package com.signalcollect.triplerush.cluster

import scala.collection.JavaConversions.asScalaIterator

import org.scalatest.Finders
import org.scalatest.fixture.FlatSpec
import org.scalatest.fixture.UnitFixture

import com.signalcollect.triplerush.DistributedTestStore
import com.signalcollect.triplerush.Lubm
import com.signalcollect.triplerush.sparql.Sparql

class ClusterSpec extends FlatSpec with UnitFixture {

  "DistributedTripleRush" should "load a triple and execute a simple SPARQL query" in new DistributedTestStore {
    tr.addStringTriple("http://s", "http://p", "http://o")
    val result = Sparql("SELECT ?s { ?s <http://p> <http://o> }")
    assert(result.hasNext, "There was no query result, but there should be one.")
    val next = result.next
    assert(!result.hasNext, "There should be exactly one result, but found more.")
    assert(next.get("s").toString == "http://s")
    Lubm.load(tr, 0)
    for { query <- Lubm.sparqlQueries } {
      val numberOfResults = Sparql(query).size
    }
  }

}
