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

package com.signalcollect.triplerush.sparql

import scala.collection.JavaConversions.asScalaIterator

import org.scalatest.fixture.{ FlatSpec, UnitFixture }

import com.signalcollect.triplerush.TestStore

class IriSpec extends FlatSpec with UnitFixture {

  "TripleRush" should "support uncommon IRIs" in new TestStore {
    val urn = "urn:uuid:123"
    tr.addStringTriple(urn, urn, "\"test\"")
    val sparql = """
SELECT ?p WHERE {
  {
      <urn:uuid:123> ?p ?o .
  }
}"""
    val results = Sparql(sparql)
    val resultBindings = results.map(_.get("p").toString).toList
    assert(resultBindings === List(urn))
  }

  it should "support uncommon IRIs when added via Jena" in new TestStore {
    val urn = "urn:uuid:123"
    val resource = model.createResource(urn)
    val property = model.createProperty(urn)
    val literal = model.createLiteral("test")
    model.add(resource, property, literal)
    val sparql = """
SELECT ?p WHERE {
  {
      <urn:uuid:123> ?p ?o .
  }
}"""
    val results = Sparql(sparql)
    val resultBindings = results.map(_.get("p").toString).toList
    assert(resultBindings === List(urn))
  }

}
