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

package com.signalcollect.triplerush

import scala.collection.JavaConversions.asScalaIterator

import org.scalatest.Finders
import org.scalatest.fixture.{ FlatSpec, UnitFixture }

import com.signalcollect.triplerush.sparql.Sparql

class SparqlSpec extends FlatSpec with UnitFixture {

  "Sparql" should "correctly translate a SPARQL query that has results" in new TestStore {
    tr.addStringTriple("http://a", "http://b", "http://c")
    tr.addStringTriple("http://a", "http://d", "http://e")
    val query = """
SELECT ?X
WHERE {
    	?X <http://b> <http://c> .
    	?X <http://d> <http://e>
}
                  """
    val results = Sparql(query)
    val decodedResults = results.map(_.get("X").toString)
    assert(decodedResults.toSet === Set("http://a"))
  }

  it should "correctly translate a SPARQL query that has no results" in new TestStore {
    tr.addStringTriple("http://a", "http://b", "http://c")
    tr.addStringTriple("http://f", "http://d", "http://e")
    val query = """
SELECT ?X
WHERE {
    	?X <http://b> <http://c> .
    	?X <http://d> <http://e>
}
                  """
    val results = Sparql(query)
    val decodedResults = results.map(_.get("X").toString)
    assert(decodedResults.toSet === Set())
  }

  it should "correctly eliminate a SPARQL query that is guaranteed to have no results" in new TestStore {
    tr.addStringTriple("http://a", "http://b", "http://c")
    tr.addStringTriple("http://f", "http://d", "http://e")
    val query = """
SELECT ?X
WHERE {
    	?X <http://z> <http://c> .
    	?X <http://d> <http://e>
}
                  """
    val results = Sparql(query)
    assert(results.toList == Nil)
  }

}
