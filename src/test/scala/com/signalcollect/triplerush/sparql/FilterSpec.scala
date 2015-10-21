/*
 *  @author Philip Stutz
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

package com.signalcollect.triplerush.sparql

import org.scalatest.{ Finders, Matchers }
import org.scalatest.fixture.{ FlatSpec, UnitFixture }
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.TestStore

class FilterSpec extends FlatSpec with UnitFixture with Matchers {

  "ARQ FILTER" should "return no result when all variables are bound during an EXISTS check and the checked thing does not exist" in new TestStore {
    val sparql = """
SELECT ?r {
  ?r <http://p> "r2"
  FILTER EXISTS { <http://r1> <http://p> <http://r3> }
}"""
    tr.addStringTriple("http://r1", "http://p", "\"r2\"")
    val results = Sparql(sparql)
    assert(!results.hasNext)
  }

  it should "return a result when all variables are bound during an EXISTS check and the checked thing does exist" in new TestStore {
    val sparql = """
SELECT ?r {
  ?r <http://p> "r2"
  FILTER EXISTS { <http://r1> <http://p> "r2" }
}"""
    tr.addStringTriple("http://r1", "http://p", "\"r2\"")
    val results = Sparql(sparql)
    assert(results.hasNext)
    assert(results.next.get("r").toString == "http://r1")
  }

  it should "return a result when all variables are bound during a NOT EXISTS check and the checked thing does not exist" in new TestStore {
    val sparql = """
SELECT ?r {
  ?r <http://p> "r2"
  FILTER NOT EXISTS { <http://r1> <http://p> <http://r3> }
}"""
    tr.addStringTriple("http://r1", "http://p", "\"r2\"")
    val results = Sparql(sparql)
    assert(results.hasNext)
    assert(results.next.get("r").toString == "http://r1")
  }

  it should "return no result when all variables are bound during a NOT EXISTS check and the checked thing does exist" in new TestStore {
    val sparql = """
SELECT ?r {
  ?r <http://p> "r2"
  FILTER NOT EXISTS { <http://r1> <http://p> "r2" }
}"""
    tr.addStringTriple("http://r1", "http://p", "\"r2\"")
    val results = Sparql(sparql)
    assert(!results.hasNext)
  }

  it should "support filtering around dates (negative)" in new TestStore {
    val sparql = """
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?r ?start {
  ?r <http://p> ?start
  FILTER (xsd:dateTime(?start) > xsd:dateTime("2010-01-01T00:00:00"))
}"""
    tr.addStringTriple("http://r1", "http://p", "\"2009-01-01T00:00:00\"")
    val results = Sparql(sparql)
    assert(!results.hasNext)
  }

  it should "support filtering around dates (positive)" in new TestStore {
    val sparql = """
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?r ?start {
  ?r <http://p> ?start
  FILTER (xsd:dateTime(?start) > xsd:dateTime("2010-01-01T00:00:00"))
}"""
    tr.addStringTriple("http://r1", "http://p", "\"2011-01-01T00:00:00\"")
    val results = Sparql(sparql)
    assert(results.hasNext)
  }

}
