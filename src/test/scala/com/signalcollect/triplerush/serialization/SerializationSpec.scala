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

package com.signalcollect.triplerush.serialization

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.signalcollect.triplerush.TestAnnouncements
import com.signalcollect.triplerush.TripleRush
import scala.concurrent.duration._
import collection.JavaConversions._
import com.signalcollect.GraphBuilder
import com.signalcollect.triplerush.sparql.Sparql

class SerializationSpec extends FlatSpec with Matchers with TestAnnouncements {

  "TripleRush serialization" should "support full message serialization" in {
    val sparql = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?product ?label
WHERE {
  ?product rdfs:label ?label .
}
ORDER BY ?label
LIMIT 3
"""
    implicit val tr = new TripleRush(graphBuilder = GraphBuilder.withMessageSerialization(true))
    try {
      tr.addTriple("http://SomeProduct2", "http://www.w3.org/2000/01/rdf-schema#label", "http://B")
      tr.addTriple("http://SomeProduct3", "http://www.w3.org/2000/01/rdf-schema#label", "http://C")
      tr.addTriple("http://SomeProduct4", "http://www.w3.org/2000/01/rdf-schema#label", "http://D")
      tr.addTriple("http://SomeProduct5", "http://www.w3.org/2000/01/rdf-schema#label", "http://E")
      tr.addTriple("http://SomeProduct1", "http://www.w3.org/2000/01/rdf-schema#label", "http://A")
      tr.prepareExecution
      val query = Sparql(sparql).get
      val result = query.resultIterator.map(bindings => (bindings("label"), bindings("product"))).toList
      assert(result === List(("http://A", "http://SomeProduct1"), ("http://B", "http://SomeProduct2"), ("http://C", "http://SomeProduct3")))
    } finally {
      tr.shutdown
    }
  }

}