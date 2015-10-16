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

import scala.collection.JavaConversions.asScalaIterator

import org.scalatest.{ Finders, Matchers }
import org.scalatest.fixture.{ FlatSpec, UnitFixture }

import com.signalcollect.triplerush.TestStore

class DistinctSpec extends FlatSpec with UnitFixture with Matchers {

  "ARQ DISTINCT" should "eliminate results with same bindings" in new TestStore {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?name WHERE { ?x foaf:name ?name }"""
    tr.addStringTriple("http://SomePerson", "http://xmlns.com/foaf/0.1/name", "\"Harold\"")
    tr.addStringTriple("http://SomeOtherPerson", "http://xmlns.com/foaf/0.1/name", "\"Harold\"")
    tr.addStringTriple("http://ThatGuy", "http://xmlns.com/foaf/0.1/name", "\"Arthur\"")
    val results = Sparql(sparql)
    assert(results.size === 2)
  }

  it should "correctly count the number of results" in new TestStore {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT (COUNT(DISTINCT ?name) as ?count) WHERE { ?x foaf:name ?name }"""
    tr.addStringTriple("http://SomePerson", "http://xmlns.com/foaf/0.1/name", "\"Harold\"")
    tr.addStringTriple("http://SomeOtherPerson", "http://xmlns.com/foaf/0.1/name", "\"Harold\"")
    tr.addStringTriple("http://ThatGuy", "http://xmlns.com/foaf/0.1/name", "\"Arthur\"")
    val results = Sparql(sparql)
    assert(results.hasNext === true)
    val bindings = results.next
    val count = bindings.get("count").asLiteral.getInt
    assert(count === 2)
    assert(results.hasNext === false)
  }

}
