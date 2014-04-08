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

class ParserSpec extends FlatSpec with Matchers with TestAnnouncements {

  "Sparql parser" should "parse a simple query" in {
    val q = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE { ?x foaf:name ?name }
"""
    val parsed = SparqlParser.parse(q)
    assert(parsed === ParsedSparqlQuery(
      List(PrefixDeclaration("foaf", """http://xmlns.com/foaf/0.1/""")),
      Select(List(Variable("name")),
        List(List(ParsedPattern(Variable("x"), Bound("foaf:name"), Variable("name")))), false)))
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
    assert(parsed === ParsedSparqlQuery(List(),
      Select(List(Variable("T"), Variable("A"), Variable("B")),
        List(List(
          ParsedPattern(Bound("""http://dbpedia.org/resource/Elvis"""), Bound("""http://dbpedia.org/property/wikilink"""), Variable("A")),
          ParsedPattern(Variable("A"), Bound("""http://dbpedia.org/property/wikilink"""), Variable("B")),
          ParsedPattern(Variable("B"), Bound("""http://dbpedia.org/property/wikilink"""), Variable("T")))), false)))
  }

  it should "support the DISTINCT keyword" in {
    val q1 = """
SELECT DISTINCT ?T ?A ?B
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
"""
    val parsed = SparqlParser.parse(q1)
    assert(parsed.select.isDistinct === true)
  }

  it should "support the UNION keyword" in {
    val q1 = """
SELECT ?property ?hasValue ?isValueOf
WHERE {
 { <http://dbpedia.org/resource/Elvis> ?property ?hasValue }
 UNION
 { ?isValueOf ?property <http://dbpedia.org/resource/Memphis> }
}
"""
    val parsed = SparqlParser.parse(q1)
    assert(parsed.select.patternUnions === List(
      List(
        ParsedPattern(Bound("""http://dbpedia.org/resource/Elvis"""), Variable("property"), Variable("hasValue"))),
      List(
        ParsedPattern(Variable("isValueOf"), Variable("property"), Bound("""http://dbpedia.org/resource/Memphis""")))))
  }

  it should "support the LIMIT keyword" in {
    val q1 = """
SELECT ?A, ?B, ?T
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
LIMIT 10;
"""
    val parsed = SparqlParser.parse(q1)
    assert(parsed.select.limit === Some(10))
  }

}
