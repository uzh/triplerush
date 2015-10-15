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
import com.signalcollect.util.TestAnnouncements

class BgpSpec extends FlatSpec with UnitFixture with Matchers with TestAnnouncements {

  "ARQ BGP" should "return the results of a simple BGP query" in new TestStore {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name ?project WHERE {
  {
      ?person foaf:name ?name .
      ?person foaf:currentProject ?project
  }
}"""
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "\"Bob\"")
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "\"Gardening\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "\"Volleyball\"")
    val results = Sparql(sparql)
    val resultBindings = results.map(_.get("project").asLiteral.getString).toSet
    assert(resultBindings === Set("Gardening", "Volleyball"))
  }

  it should "support BGPs with OPTIONAL, FILTER, and BOUND" in new TestStore {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name ?project WHERE {
  {
      ?person foaf:name ?name .
      OPTIONAL { ?person foaf:currentProject ?project }
      FILTER(!BOUND(?project))
  }
}"""
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "\"Bob\"")
    tr.addStringTriple("http://PersonC", "http://xmlns.com/foaf/0.1/name", "\"Caroline\"")
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "\"Gardening\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "\"Volleyball\"")
    val results = Sparql(sparql)
    val resultBindings = results.map(_.get("name").asLiteral.getString).toSet
    assert(resultBindings === Set("Caroline"))
  }

  it should "support a nested query with GROUP BY and FILTER" in new TestStore {
    val sparql = """
  PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
  SELECT ?person ?projectCount
  WHERE {
    {
      SELECT ?person (COUNT(?project) as ?projectCount)
      WHERE {
        ?person foaf:name ?name .
        ?person foaf:currentProject ?project .
      }
      GROUP BY ?person
    }
    FILTER (?projectCount > 1)
  }"""
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "\"Bob\"")
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "\"Gardening\"")
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "\"Skydiving\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "\"Volleyball\"")
    val results = Sparql(sparql)
    val resultBindings = results.map { bindings =>
      bindings.getResource("person").toString
    }.toSet
    assert(resultBindings === Set("http://PersonA"))
  }

  /**
   * Example from spec: http://www.w3.org/TR/sparql11-query/#subqueries
   */
  it should "support a nested query with MIN aggregation" in new TestStore {
    val sparql = """
  PREFIX : <http://people.example/>
  SELECT ?y ?minName
  WHERE {
    :alice :knows ?y .
    {
      SELECT ?y (MIN(?name) AS ?minName)
      WHERE {
        ?y :name ?name .
      } GROUP BY ?y
    }
  }"""
    tr.addStringTriple("http://people.example/alice", "http://people.example/knows", "http://people.example/bob")
    tr.addStringTriple("http://people.example/alice", "http://people.example/knows", "http://people.example/carol")
    tr.addStringTriple("http://people.example/bob", "http://people.example/name", "\"Bob\"")
    tr.addStringTriple("http://people.example/bob", "http://people.example/name", "\"Bob Bar\"")
    tr.addStringTriple("http://people.example/bob", "http://people.example/name", "\"B. Bar\"")
    tr.addStringTriple("http://people.example/carol", "http://people.example/name", "\"Carol\"")
    tr.addStringTriple("http://people.example/carol", "http://people.example/name", "\"Carol Baz\"")
    tr.addStringTriple("http://people.example/carol", "http://people.example/name", "\"C. Baz\"")
    val results = Sparql(sparql)
    val resultBindings = results.map { bindings =>
      bindings.getLiteral("minName").getString
    }.toSet
    assert(resultBindings === Set("B. Bar", "C. Baz"))
  }

}
