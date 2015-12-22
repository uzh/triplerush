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

import org.apache.jena.rdf.model.RDFNode
import org.scalatest.{ Finders, Matchers }
import org.scalatest.fixture.{ FlatSpec, UnitFixture }

import com.signalcollect.triplerush.TestStore

class NestedOptionalSpec extends FlatSpec with UnitFixture with Matchers {

  "ARQ Nested" should "optionally return people who have the same name as their project" in new TestStore {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name ?project ?friend WHERE {
  ?person foaf:name ?name .
  OPTIONAL {
    ?person foaf:currentProject ?project
    OPTIONAL {
      ?person foaf:friendOf ?friend
    }
  }
}"""
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "\"Bob\"")
    tr.addStringTriple("http://PersonC", "http://xmlns.com/foaf/0.1/name", "\"Carol\"")
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "http://Gardening")
    tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "http://Volleyball")
    tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/friendOf", "http://PersonB")
    tr.addStringTriple("http://PersonC", "http://xmlns.com/foaf/0.1/friendOf", "http://PersonA")
    val results = Sparql(sparql)
    def stringify(n: RDFNode): String = Option(n).map(_.toString).getOrElse("")
    val resultBindings = results.map { result =>
      List(stringify(result.get("name")), stringify(result.get("project")), stringify(result.get("friend")))
    }
    assert(resultBindings.toSet === Set(List("Carol", "", ""), List("Arnie", "http://Gardening", "http://PersonB"), List("Bob", "http://Volleyball", "")))
  }

}
