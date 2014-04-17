/*
 *  @author Philip Stutz
 *  
 *  Copyright 2014 University of Zurich
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

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.signalcollect.triplerush.TestAnnouncements
import com.signalcollect.triplerush.TripleRush

class ParserSpec extends FlatSpec with Matchers with TestAnnouncements {

  "Sparql parser" should "parse a simple query" in {
    val q = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE { ?x foaf:name ?name }
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed === ParsedSparqlQuery(
      Map("foaf" -> """http://xmlns.com/foaf/0.1/"""),
      Select(List("name"),
        List(List(ParsedPattern(Variable("x"), Iri("foaf:name"), Variable("name")))), false)))
  }

  it should "parse a query with multiple patterns" in {
    val q = """
SELECT ?T ?A ?B
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed === ParsedSparqlQuery(Map(),
      Select(List("T", "A", "B"),
        List(List(
          ParsedPattern(Iri("""http://dbpedia.org/resource/Elvis"""), Iri("""http://dbpedia.org/property/wikilink"""), Variable("A")),
          ParsedPattern(Variable("A"), Iri("""http://dbpedia.org/property/wikilink"""), Variable("B")),
          ParsedPattern(Variable("B"), Iri("""http://dbpedia.org/property/wikilink"""), Variable("T")))), false)))
  }

  it should "support the DISTINCT keyword" in {
    val q = """
SELECT DISTINCT ?T ?A ?B
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed.select.isDistinct === true)
  }

  it should "support the UNION keyword" in {
    val q = """
SELECT ?property ?hasValue ?isValueOf
WHERE {
 { <http://dbpedia.org/resource/Elvis> ?property ?hasValue }
 UNION
 { ?isValueOf ?property <http://dbpedia.org/resource/Memphis> }
}
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed.select.patternUnions === List(
      List(
        ParsedPattern(Iri("""http://dbpedia.org/resource/Elvis"""), Variable("property"), Variable("hasValue"))),
      List(
        ParsedPattern(Variable("isValueOf"), Variable("property"), Iri("""http://dbpedia.org/resource/Memphis""")))))
  }

  it should "support the LIMIT keyword" in {
    val q = """
SELECT ?A, ?B, ?T
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
LIMIT 10;
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed.select.limit === Some(10))
  }

  it should "support the ORDER BY keyword" in {
    val q = """
PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
  ?product a <http://dbpedia.org/resource/Elvis> .
  ?product bsbm:productFeature <http://dbpedia.org/resource/Madonna> .
}
ORDER BY ?label
LIMIT 10
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed.select.orderBy === Some("label"))
  }

  it should "support underlines in queries" in {
    val q = """
SELECT ?T ?A
WHERE {
		  <http://dbpedia.org/resource/Some_Person> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?T
}
"""
    val parsed = SparqlParser.parse(q)
  }

  it should "support abbreviating rdf:type with a" in {
    val q = """
SELECT ?T ?A
WHERE {
		  <http://dbpedia.org/resource/Some_Person> a ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?T
}
"""
    val tr = new TripleRush
    try {
      tr.addTriple("http://dbpedia.org/resource/Some_Person", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://dbpedia.org/resource/Thing")
      tr.addTriple("http://dbpedia.org/resource/Thing", "http://dbpedia.org/property/wikilink", "Link")
      tr.prepareExecution
      val query = Sparql(q)(tr).get
      val result = query.resultIterator
      assert(result.toList.map(_("A")) === List("http://dbpedia.org/resource/Thing"))
    } finally {
      tr.shutdown
    }
  }

}
