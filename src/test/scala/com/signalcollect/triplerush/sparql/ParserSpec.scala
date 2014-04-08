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

  "Sparql parser" should "parse simple queries" in {
    val q1 = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE { ?x foaf:name ?name }
"""
    val parsed1 = SparqlParser.parse(q1)

    val q2 = """
SELECT ?T ?A ?B
WHERE {
		  <http://dbpedia.org/resource/Elvis> <http://dbpedia.org/property/wikilink> ?A .
		  ?A <http://dbpedia.org/property/wikilink> ?B .
		  ?B <http://dbpedia.org/property/wikilink> ?T
}
"""
    val parsed2 = SparqlParser.parse(q2)
    println(parsed2)
    true
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

}
