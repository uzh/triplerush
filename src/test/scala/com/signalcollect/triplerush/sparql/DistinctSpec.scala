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
import com.signalcollect.triplerush.QuerySpecification
import com.signalcollect.triplerush.TripleRush
import scala.concurrent.Await
import scala.concurrent.duration._

class DistinctSpec extends FlatSpec with Matchers with TestAnnouncements {

  "DISTNCT keyword" should "be correctly parsed" in {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?name WHERE { ?x foaf:name ?name }
"""
    val tr = new TripleRush
    tr.addTriple("http://SomePerson", "http://xmlns.com/foaf/0.1/name", "Harold")
    tr.prepareExecution
    try {
      val query = QuerySpecification.fromSparql(sparql).get
      assert(query.distinct === true)
    } finally {
      tr.shutdown
    }
  }

  it should "eliminate results with same bindings" in {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?name WHERE { ?x foaf:name ?name }
"""
    val tr = new TripleRush
    tr.addTriple("http://SomePerson", "http://xmlns.com/foaf/0.1/name", "Harold")
    tr.addTriple("http://SomeOtherPerson", "http://xmlns.com/foaf/0.1/name", "Harold")
    tr.addTriple("http://ThatGuy", "http://xmlns.com/foaf/0.1/name", "Arthur")
    tr.prepareExecution
    try {
      val query = QuerySpecification.fromSparql(sparql).get
      assert(query.distinct === true)
      val result = tr.executeQuery(query)
      assert(result.size === 2)
    } finally {
      tr.shutdown
    }
  }

  it should "correctly count the number of results" in {
    val sparql = """
PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?name WHERE { ?x foaf:name ?name }
"""
    val tr = new TripleRush
    tr.addTriple("http://SomePerson", "http://xmlns.com/foaf/0.1/name", "Harold")
    tr.addTriple("http://SomeOtherPerson", "http://xmlns.com/foaf/0.1/name", "Harold")
    tr.addTriple("http://ThatGuy", "http://xmlns.com/foaf/0.1/name", "Arthur")
    tr.prepareExecution
    try {
      val query = QuerySpecification.fromSparql(sparql).get
      assert(query.distinct === true)
      val resultFuture = tr.executeCountingQuery(query)
      val result = Await.result(resultFuture, 10.seconds)
      assert(result.get === 2)
    } finally {
      tr.shutdown
    }
  }

}
